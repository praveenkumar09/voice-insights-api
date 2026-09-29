package com.aia.voiceinsights.api.model;

/**
 * One SSE frame streamed to voice-insights-ui over
 * GET /api/recommendations/{runId}/stream, driving the live pipeline
 * animation. {@code agent} is one of RecommendationStore.PIPELINE_ORDER's
 * keys ("need", "risk", "affordability", "merge", "persona",
 * "productScoring", "productShortlist", "ragValidation", "complianceCheck",
 * "summary", "salesReport") or null for run-level events. Every step
 * — including merge — uses this same generic started/completed/failed
 * vocabulary rather than step-specific event types, since the pipeline is
 * expected to keep growing and a bespoke event pair per step doesn't scale
 * with that. {@code payload} is the step's result once type is
 * agent_completed.
 */
public record RecommendationEvent(String type, String agent, Object payload) {

    public static RecommendationEvent runStarted(String runId) {
        return new RecommendationEvent("run_started", null, java.util.Map.of("runId", runId));
    }

    public static RecommendationEvent agentStarted(String agent) {
        return new RecommendationEvent("agent_started", agent, null);
    }

    public static RecommendationEvent agentCompleted(String agent, Object result) {
        return new RecommendationEvent("agent_completed", agent, result);
    }

    public static RecommendationEvent agentFailed(String agent, String message) {
        return new RecommendationEvent("agent_failed", agent, java.util.Map.of("error", message));
    }

    public static RecommendationEvent runFailed(String message) {
        return new RecommendationEvent("run_failed", null, java.util.Map.of("error", message));
    }
}
