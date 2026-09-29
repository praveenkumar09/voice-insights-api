package com.aia.voiceinsights.api.voice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Relays browser microphone audio to OpenAI's Realtime transcription
 * endpoint over a server-side websocket (the JDK's own {@link WebSocket}
 * client — Spring AI 1.0.0-M6 has no realtime-audio client), and surfaces
 * partial/final transcript events back to {@link Listener}.
 *
 * Protocol (OpenAI Realtime API, GA — the old Beta shape, including the
 * {@code OpenAI-Beta: realtime=v1} header, is rejected outright as of the
 * Beta sunset; see https://developers.openai.com/api/docs/guides/realtime-transcription):
 *   client -> server: {"type":"session.update","session":{"type":"transcription","audio":{"input":{...}}}}
 *                     once on open, then {"type":"input_audio_buffer.append","audio":"<base64 pcm16>"}
 *                     per chunk, and {"type":"input_audio_buffer.commit"} to end a turn.
 *   server -> client: "conversation.item.input_audio_transcription.delta" (partial),
 *                     "conversation.item.input_audio_transcription.completed" (final),
 *                     "error".
 */
public class OpenAiRealtimeTranscriptionClient {

    public interface Listener {
        void onPartialTranscript(String text);
        void onFinalTranscript(String text);
        void onError(String message);
        void onClose();
    }

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private final StringBuilder frameBuffer = new StringBuilder();

    private final String apiUrl;
    private final String apiKey;
    private final String transcriptionModel;
    private final String prompt;
    private final Listener listener;

    private volatile WebSocket webSocket;

    // The browser starts streaming audio as soon as ITS websocket to us opens,
    // which can beat our own async handshake to OpenAI — any chunk that
    // arrives before `webSocket` is set here was previously just dropped
    // (send()'s null check), silently clipping the start of the very first
    // utterance of a session. Queue instead, flush in order once connected.
    private final Queue<byte[]> pendingAudio = new ConcurrentLinkedQueue<>();

    /**
     * {@code prompt}: free-text context to bias transcription vocabulary (see
     * sendSessionUpdate). {@code languages} and {@code keywords} — both
     * documented as bias mechanisms for some transcription models — are
     * deliberately NOT supported here: confirmed live against gpt-4o-transcribe
     * that both are rejected outright ("The 'languages'/'keywords' parameter is
     * not supported for this model"). prompt alone was confirmed accepted.
     */
    public OpenAiRealtimeTranscriptionClient(String apiUrl, String apiKey, String transcriptionModel,
                                              String prompt, Listener listener) {
        this.apiUrl = apiUrl;
        this.apiKey = apiKey;
        this.transcriptionModel = transcriptionModel;
        this.prompt = prompt;
        this.listener = listener;
    }

    /** Transcription sessions only support 24kHz PCM input — GA schema requirement, not configurable. */
    private static final int INPUT_SAMPLE_RATE_HZ = 24000;

    /**
     * Server-side VAD's natural pause detection, not a fixed length — 500ms
     * is comfortably past a normal mid-sentence breath without making the
     * agent wait long after they actually stop talking.
     */
    private static final int SILENCE_DURATION_MS = 500;

    public CompletableFuture<Void> connect() {
        return httpClient.newWebSocketBuilder()
                .header("Authorization", "Bearer " + apiKey)
                .buildAsync(URI.create(apiUrl), new RealtimeListener())
                .thenAccept(ws -> {
                    this.webSocket = ws;
                    sendSessionUpdate();
                    byte[] queued;
                    while ((queued = pendingAudio.poll()) != null) {
                        sendAudioChunk(queued);
                    }
                });
    }

    /**
     * GA session.update shape (the old beta shape — top-level input_audio_format /
     * input_audio_transcription — is rejected by the current API).
     *
     * turn_detection is server_vad, not null: with it null, OpenAI only ever
     * finalizes a transcript in response to an explicit input_audio_buffer.commit,
     * so committing on any fixed schedule (a timer, or once at session end)
     * inevitably cuts mid-word at that boundary. server_vad instead finalizes
     * automatically at real pauses in speech (confirmed empirically — see
     * conversation history: a two-sentence test with a real pause produced two
     * word-perfect transcripts with zero manual commits), which is both simpler
     * and produces complete, uncut text.
     *
     * prompt biases transcription toward Singapore/Malaysia/China insurance-
     * conversation vocabulary (Singlish/Manglish, local names, CPF/MediSave
     * etc.) — see voice.openai.realtime.prompt.
     */
    private void sendSessionUpdate() {
        Map<String, Object> format = new LinkedHashMap<>();
        format.put("type", "audio/pcm");
        format.put("rate", INPUT_SAMPLE_RATE_HZ);

        Map<String, Object> transcription = new LinkedHashMap<>();
        transcription.put("model", transcriptionModel);
        if (prompt != null && !prompt.isBlank()) transcription.put("prompt", prompt);

        Map<String, Object> turnDetection = new LinkedHashMap<>();
        turnDetection.put("type", "server_vad");
        turnDetection.put("silence_duration_ms", SILENCE_DURATION_MS);

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("format", format);
        input.put("transcription", transcription);
        input.put("turn_detection", turnDetection);

        Map<String, Object> audio = Map.of("input", input);
        Map<String, Object> session = Map.of("type", "transcription", "audio", audio);
        send(Map.of("type", "session.update", "session", session));
    }

    /** {@code pcm16}: raw 24kHz, 16-bit mono PCM samples (see voice-insights-ui's useVoiceCapture hook). */
    public void sendAudioChunk(byte[] pcm16) {
        if (webSocket == null) {
            pendingAudio.add(pcm16);
            return;
        }
        send(Map.of("type", "input_audio_buffer.append", "audio", Base64.getEncoder().encodeToString(pcm16)));
    }

    public void commit() {
        send(Map.of("type", "input_audio_buffer.commit"));
    }

    public void close() {
        WebSocket ws = webSocket;
        if (ws != null) ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
    }

    private void send(Map<String, Object> payload) {
        WebSocket ws = webSocket;
        if (ws == null) return;
        try {
            ws.sendText(mapper.writeValueAsString(payload), true);
        } catch (Exception e) {
            listener.onError("Failed to send to OpenAI realtime: " + e.getMessage());
        }
    }

    private void handleEvent(String json) {
        try {
            JsonNode root = mapper.readTree(json);
            String type = root.path("type").asText("");
            switch (type) {
                case "conversation.item.input_audio_transcription.delta" -> {
                    String delta = root.path("delta").asText("");
                    if (!delta.isBlank()) listener.onPartialTranscript(delta);
                }
                case "conversation.item.input_audio_transcription.completed" -> {
                    String transcript = root.path("transcript").asText("");
                    if (!transcript.isBlank()) listener.onFinalTranscript(transcript);
                }
                case "error" -> listener.onError(root.path("error").path("message").asText(json));
                default -> { /* session.created, session.updated, etc. — ignored */ }
            }
        } catch (Exception e) {
            listener.onError("Failed to parse OpenAI realtime event: " + e.getMessage());
        }
    }

    private class RealtimeListener implements WebSocket.Listener {
        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            frameBuffer.append(data);
            webSocket.request(1);
            if (last) {
                String full = frameBuffer.toString();
                frameBuffer.setLength(0);
                handleEvent(full);
            }
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            listener.onError(error.getMessage());
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            listener.onClose();
            return null;
        }
    }
}
