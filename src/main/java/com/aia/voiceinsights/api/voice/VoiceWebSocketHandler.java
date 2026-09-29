package com.aia.voiceinsights.api.voice;

import com.aia.voiceinsights.api.model.CustomerProfile;
import com.aia.voiceinsights.api.service.CustomerProfileStore;
import com.aia.voiceinsights.api.service.ProfileExtractionService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * WS /ws/voice — one connection per voice-capture session. The browser
 * streams raw 24kHz/16-bit mono PCM audio frames up (binary) as the agent
 * talks with a customer; this handler relays them to OpenAI's realtime
 * transcription model (see {@link OpenAiRealtimeTranscriptionClient}),
 * streams partial/final transcript text back down (text/JSON), and re-runs
 * {@link ProfileExtractionService} after each final segment so the customer
 * profile fills in live.
 *
 * Transcription-model sessions use server-side VAD ({@code turn_detection:
 * server_vad} — see OpenAiRealtimeTranscriptionClient) to finalize a
 * transcript automatically at real pauses in speech. An earlier version used
 * {@code turn_detection: null} plus a fixed-interval manual commit, which cut
 * transcripts at arbitrary timer boundaries instead of actual pauses,
 * silently dropping whatever words spanned a cut. server_vad both fixes that
 * and removes the need for this class to drive commits itself.
 *
 * OpenAI's realtime sessions are not indefinite — a session that runs long
 * enough gets closed server-side. Without handling that, a long conversation
 * would just go silent (no more transcript events) with no visible error, at
 * whatever point the session expired. {@link #connectTranscriptionClient}
 * is used for both the initial connection and, via the {@code onClose}
 * listener, to transparently reconnect a fresh OpenAI session mid-call —
 * {@code vs.transcript} keeps accumulating server-side across reconnects, so
 * the agent never sees a gap beyond the brief reconnect itself. Reconnects
 * are capped (see MAX_RECONNECT_ATTEMPTS) so a genuinely broken upstream
 * (bad key, OpenAI outage) surfaces as an error instead of retrying forever.
 */
@Component
public class VoiceWebSocketHandler extends AbstractWebSocketHandler {

    private static final int MAX_RECONNECT_ATTEMPTS = 5;

    private final CustomerProfileStore profileStore;
    private final ProfileExtractionService extractionService;
    // findAndRegisterModules() picks up JSR-310 support (java.time.Instant) —
    // without it, serializing CustomerProfile (createdAt/updatedAt) throws,
    // and sendJsonQuiet's catch-all silently drops every "profile" message.
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final ExecutorService extractionExecutor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService reconnectScheduler = Executors.newScheduledThreadPool(2);
    private final Map<String, VoiceSession> sessions = new ConcurrentHashMap<>();

    @Value("${voice.openai.realtime.url}")
    private String realtimeUrl;

    @Value("${voice.openai.realtime.transcription-model}")
    private String transcriptionModel;

    @Value("${voice.openai.realtime.prompt:}")
    private String transcriptionPrompt;

    @Value("${OPENAI_API_KEY:}")
    private String openaiApiKey;

    public VoiceWebSocketHandler(CustomerProfileStore profileStore, ProfileExtractionService extractionService) {
        this.profileStore = profileStore;
        this.extractionService = extractionService;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession wsSession) throws Exception {
        CustomerProfile profile = profileStore.save(new CustomerProfile());
        VoiceSession vs = new VoiceSession(profile);
        sessions.put(wsSession.getId(), vs);

        if (openaiApiKey == null || openaiApiKey.isBlank()) {
            sendJsonQuiet(wsSession, Map.of("type", "error", "message", "OPENAI_API_KEY not configured on the server"));
        } else {
            connectTranscriptionClient(wsSession, vs);
        }

        sendJsonQuiet(wsSession, Map.of("type", "session_started", "customerProfileId", vs.profile.getId()));
    }

    private void connectTranscriptionClient(WebSocketSession wsSession, VoiceSession vs) {
        vs.transcriptionClient = new OpenAiRealtimeTranscriptionClient(realtimeUrl, openaiApiKey, transcriptionModel,
                transcriptionPrompt,
                new OpenAiRealtimeTranscriptionClient.Listener() {
                    @Override
                    public void onPartialTranscript(String text) {
                        sendJsonQuiet(wsSession, Map.of("type", "partial_transcript", "text", text));
                    }

                    @Override
                    public void onFinalTranscript(String text) {
                        vs.reconnectAttempts = 0; // a real transcript flowed — the connection is healthy again
                        vs.transcript.append(text).append(" ");
                        sendJsonQuiet(wsSession, Map.of("type", "final_transcript", "text", text));
                        extractionExecutor.submit(() -> reExtractAndPush(wsSession, vs));
                    }

                    @Override
                    public void onError(String message) {
                        sendJsonQuiet(wsSession, Map.of("type", "error", "message", message));
                    }

                    @Override
                    public void onClose() {
                        if (vs.intentionalClose || !wsSession.isOpen()) return;

                        if (vs.reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
                            sendJsonQuiet(wsSession, Map.of("type", "error",
                                    "message", "Lost connection to the transcription service and could not reconnect."));
                            return;
                        }
                        vs.reconnectAttempts++;
                        // Backs off a little on repeated failures (network blip vs. a
                        // session that just hit its natural expiry, which reconnects
                        // cleanly on the first try) without ever going silent on the agent.
                        long delaySeconds = Math.min(vs.reconnectAttempts, 3);
                        reconnectScheduler.schedule(() -> connectTranscriptionClient(wsSession, vs),
                                delaySeconds, TimeUnit.SECONDS);
                    }
                });
        vs.transcriptionClient.connect().exceptionally(ex -> {
            sendJsonQuiet(wsSession, Map.of("type", "error", "message", "Failed to connect to OpenAI realtime: " + ex.getMessage()));
            return null;
        });
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession wsSession, BinaryMessage message) {
        VoiceSession vs = sessions.get(wsSession.getId());
        if (vs == null || vs.transcriptionClient == null) return;
        byte[] bytes = new byte[message.getPayload().remaining()];
        message.getPayload().get(bytes);
        vs.transcriptionClient.sendAudioChunk(bytes);
    }

    @Override
    protected void handleTextMessage(WebSocketSession wsSession, TextMessage message) throws Exception {
        VoiceSession vs = sessions.get(wsSession.getId());
        if (vs == null) return;

        JsonNode node = mapper.readTree(message.getPayload());
        if ("stop".equals(node.path("type").asText(""))) {
            // Safety flush: server_vad auto-commits at real pauses, but if the
            // agent stops mid-utterance (no pause yet when they hit stop),
            // nothing has committed that trailing bit yet — commit it explicitly.
            if (vs.transcriptionClient != null) vs.transcriptionClient.commit();
            extractionExecutor.submit(() -> finalizeSession(wsSession, vs));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession wsSession, CloseStatus status) {
        VoiceSession vs = sessions.remove(wsSession.getId());
        if (vs == null) return;
        vs.intentionalClose = true; // the browser side is gone either way — never attempt to reconnect after this
        if (vs.transcriptionClient != null) vs.transcriptionClient.close();
    }

    private void finalizeSession(WebSocketSession wsSession, VoiceSession vs) {
        vs.intentionalClose = true;

        // Give OpenAI a moment to flush the transcription.completed event for
        // the final (safety-flush) commit above.
        try { Thread.sleep(1500); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }

        extractionService.extractInto(vs.profile, vs.transcript.toString());
        vs.profile.setRawTranscript(vs.transcript.toString());
        vs.profile.setStatus("FINALIZED");
        profileStore.save(vs.profile);

        sendJsonQuiet(wsSession, Map.of("type", "profile", "profile", vs.profile));
        sendJsonQuiet(wsSession, Map.of("type", "session_ended"));
        if (vs.transcriptionClient != null) vs.transcriptionClient.close();
        try {
            if (wsSession.isOpen()) wsSession.close(CloseStatus.NORMAL);
        } catch (Exception ignored) {
            // Best-effort — afterConnectionClosed still runs and cleans up session state.
        }
    }

    private void reExtractAndPush(WebSocketSession wsSession, VoiceSession vs) {
        extractionService.extractInto(vs.profile, vs.transcript.toString());
        profileStore.save(vs.profile);
        sendJsonQuiet(wsSession, Map.of("type", "profile", "profile", vs.profile));
    }

    private void sendJsonQuiet(WebSocketSession session, Object payload) {
        try {
            synchronized (session) {
                if (session.isOpen()) session.sendMessage(new TextMessage(mapper.writeValueAsString(payload)));
            }
        } catch (Exception e) {
            // Best-effort — a dropped status update doesn't need to fail the session,
            // but it must be visible somewhere, or a serialization bug like this one
            // (see the ObjectMapper field above) silently disappears with no trace.
            System.err.println("VoiceWebSocketHandler: failed to send " + payload + ": " + e);
        }
    }

    private static class VoiceSession {
        final CustomerProfile profile;
        volatile OpenAiRealtimeTranscriptionClient transcriptionClient;
        volatile boolean intentionalClose = false;
        volatile int reconnectAttempts = 0;
        final StringBuilder transcript = new StringBuilder();

        VoiceSession(CustomerProfile profile) {
            this.profile = profile;
        }
    }
}
