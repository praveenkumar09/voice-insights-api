package com.aia.voiceinsights.api.controller;

import com.aia.voiceinsights.api.config.SessionAuthFilter;
import com.aia.voiceinsights.api.model.RecommendationEvent;
import com.aia.voiceinsights.api.model.RecommendationRunView;
import com.aia.voiceinsights.api.model.SalesReportResult;
import com.aia.voiceinsights.api.service.RateLimiterService;
import com.aia.voiceinsights.api.service.RecommendationEventBus;
import com.aia.voiceinsights.api.service.RecommendationOrchestrationService;
import com.aia.voiceinsights.api.service.RecommendationStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api")
public class RecommendationController {

    private static final Logger log = LoggerFactory.getLogger(RecommendationController.class);

    /** Each run drives ~9 LLM calls — this caps runaway/abusive triggering per advisor account. */
    private static final int RUNS_PER_HOUR = 20;

    private final RecommendationOrchestrationService orchestrationService;
    private final RecommendationEventBus eventBus;
    private final RecommendationStore store;
    private final RateLimiterService rateLimiter;
    private final ObjectMapper mapper = new ObjectMapper();

    public RecommendationController(RecommendationOrchestrationService orchestrationService,
                                     RecommendationEventBus eventBus,
                                     RecommendationStore store,
                                     RateLimiterService rateLimiter) {
        this.orchestrationService = orchestrationService;
        this.eventBus = eventBus;
        this.store = store;
        this.rateLimiter = rateLimiter;
    }

    /** Kicks off the full 11-agent recommendation graph for a finalized customer profile. */
    @PostMapping(value = "/customers/{id}/recommendations", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> start(@PathVariable String id,
                                    @RequestAttribute(name = SessionAuthFilter.USER_ID_ATTR, required = false) String userId) {
        if (store.findLatestRun(id).filter(r -> "RUNNING".equals(r.status())).isPresent()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "A recommendation is already running for this customer"));
        }

        String rateLimitKey = userId != null ? userId : "anonymous";
        if (!rateLimiter.tryConsume("run:" + rateLimitKey, RUNS_PER_HOUR, Duration.ofHours(1))) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header(HttpHeaders.RETRY_AFTER, "3600")
                    .body(Map.of("error", "Too many recommendation runs this hour — please try again later"));
        }

        try {
            String runId = orchestrationService.startRun(id);
            log.info("Recommendation run {} started for customer {} by user {}", runId, id, userId);
            return ResponseEntity.ok(Map.of("runId", runId));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** Post-hoc, viewable snapshot of a run — works during and after the live SSE stream. */
    @GetMapping(value = "/recommendations/{runId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<RecommendationRunView> get(@PathVariable String runId) {
        return store.getRunView(runId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** The final sales report as a downloadable Markdown file. */
    @GetMapping(value = "/recommendations/{runId}/report", produces = "text/markdown;charset=UTF-8")
    public ResponseEntity<byte[]> downloadReport(@PathVariable String runId) {
        Optional<SalesReportResult> report = store.getSalesReport(runId);
        if (report.isEmpty()) return ResponseEntity.notFound().build();

        byte[] body = report.get().reportMarkdown().getBytes(StandardCharsets.UTF_8);
        String filename = "AIA-Advisory-Report-" + runId.substring(0, 8) + ".md";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, ContentDisposition.attachment().filename(filename).build().toString())
                .body(body);
    }

    /**
     * Live pipeline events for a run: run_started, agent_started,
     * agent_completed, agent_failed (one triple per step, including merge),
     * or run_failed. Drives voice-insights-ui's pipeline animation — see
     * RecommendationEventBus for the replay-buffered sink that lets a
     * subscriber arriving slightly after the POST still see every prior
     * event for this run.
     */
    // Spring MVC's Flux<String> + text/event-stream support already frames
    // each emitted String as its own "data: <element>\n\n" SSE block — a
    // manually-built "data: ...\n\n" string here gets wrapped a SECOND time,
    // producing literal doubled/malformed "data:data: ..." bytes on the wire
    // that fail JSON.parse in the browser's EventSource for every event. Just
    // emit the raw JSON text; Spring does the SSE framing.
    @GetMapping(value = "/recommendations/{runId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream(@PathVariable String runId) {
        return eventBus.subscribe(runId).map(this::toJson);
    }

    private String toJson(RecommendationEvent event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (Exception e) {
            return "{\"type\":\"error\",\"payload\":{\"error\":\"serialization failed\"}}";
        }
    }
}
