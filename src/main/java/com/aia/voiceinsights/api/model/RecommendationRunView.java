package com.aia.voiceinsights.api.model;

import java.util.List;

/** Post-hoc, viewable snapshot of a run — GET /api/recommendations/{runId}. */
public record RecommendationRunView(
    String runId,
    String customerProfileId,
    String status, // PENDING | RUNNING | COMPLETED | FAILED
    List<AgentStepView> steps,
    String errorMessage
) {}
