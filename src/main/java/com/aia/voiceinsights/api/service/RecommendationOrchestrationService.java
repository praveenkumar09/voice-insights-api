package com.aia.voiceinsights.api.service;

import com.aia.voiceinsights.api.graph.RecommendationGraphFactory;
import com.aia.voiceinsights.api.graph.RecommendationState;
import com.aia.voiceinsights.api.model.*;
import jakarta.annotation.PreDestroy;
import org.bsc.langgraph4j.CompiledGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Kicks off a recommendation run on a background thread so the POST that
 * starts it returns immediately with a runId — the caller then watches
 * progress via GET /api/recommendations/{runId}/stream (SSE, see
 * RecommendationEventBus) and/or polls GET /api/recommendations/{runId}.
 */
@Service
public class RecommendationOrchestrationService {

    private static final Logger log = LoggerFactory.getLogger(RecommendationOrchestrationService.class);

    /** Bounded, not cached/unbounded — each run occupies one thread for its whole (blocking) lifetime,
     *  so an unbounded pool lets an unexpected burst of runs exhaust resources with no ceiling. */
    private static final int MAX_CONCURRENT_RUNS = 16;

    private final RecommendationGraphFactory graphFactory;
    private final RecommendationStore store;
    private final RecommendationEventBus eventBus;
    private final CustomerProfileStore profileStore;
    private final RecommendationAgentService agentService;
    private final ExecutorService executor = new ThreadPoolExecutor(
            4, MAX_CONCURRENT_RUNS, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(200));

    public RecommendationOrchestrationService(RecommendationGraphFactory graphFactory,
                                               RecommendationStore store,
                                               RecommendationEventBus eventBus,
                                               CustomerProfileStore profileStore,
                                               RecommendationAgentService agentService) {
        this.graphFactory = graphFactory;
        this.store = store;
        this.eventBus = eventBus;
        this.profileStore = profileStore;
        this.agentService = agentService;
    }

    public String startRun(String customerProfileId) {
        CustomerProfile profile = profileStore.findById(customerProfileId)
                .orElseThrow(() -> new IllegalArgumentException("No customer profile found for id " + customerProfileId));

        String runId = UUID.randomUUID().toString();
        store.createRun(runId, customerProfileId);
        eventBus.publish(runId, RecommendationEvent.runStarted(runId));

        log.info("Recommendation run {} queued for customer {}", runId, customerProfileId);
        executor.submit(() -> runGraph(runId, profile));
        return runId;
    }

    private void runGraph(String runId, CustomerProfile profile) {
        Instant startedAt = Instant.now();
        try {
            CompiledGraph<RecommendationState> graph = graphFactory.build(runId, profile);
            RecommendationState finalState = graph.invoke(Map.of("started", true))
                    .orElseThrow(() -> new IllegalStateException("Recommendation graph produced no final state"));

            // Defensive fallback: if the merge node's output is missing from the
            // final state for any reason, synthesize it here from whatever
            // per-agent results ARE present, so a run never gets stuck without
            // a mergedInsights row even if the graph's fan-in wiring behaves
            // unexpectedly at runtime.
            if (finalState.<MergedInsights>value("mergedInsights").isEmpty()) {
                ensureMergedInsights(runId, profile, finalState);
            }

            store.markRunStatus(runId, "COMPLETED", null);
            log.info("Recommendation run {} completed in {} ms", runId, Duration.between(startedAt, Instant.now()).toMillis());
        } catch (Exception e) {
            store.markRunStatus(runId, "FAILED", e.getMessage());
            eventBus.publish(runId, RecommendationEvent.runFailed(e.getMessage()));
            log.warn("Recommendation run {} failed after {} ms: {}",
                    runId, Duration.between(startedAt, Instant.now()).toMillis(), e.getMessage(), e);
        } finally {
            eventBus.complete(runId);
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    private void ensureMergedInsights(String runId, CustomerProfile profile, RecommendationState state) {
        NeedAnalysisResult need = state.<NeedAnalysisResult>value("needResult")
                .orElseGet(() -> new NeedAnalysisResult(List.of(), List.of(), List.of(), "Not available"));
        RiskAnalysisResult risk = state.<RiskAnalysisResult>value("riskResult")
                .orElseGet(() -> new RiskAnalysisResult(List.of(), "Unknown", "Not available"));
        AffordabilityResult affordability = state.<AffordabilityResult>value("affordabilityResult")
                .orElseGet(() -> new AffordabilityResult("Unknown", "Not available", "Not available"));

        MergedInsights merged = agentService.merge(profile, need, risk, affordability);
        Map<String, Object> input = Map.of("need", need, "risk", risk, "affordability", affordability);
        store.saveAgentStep(runId, "merge", "COMPLETED", input, merged);
        eventBus.publish(runId, RecommendationEvent.agentCompleted("merge", merged));
    }
}
