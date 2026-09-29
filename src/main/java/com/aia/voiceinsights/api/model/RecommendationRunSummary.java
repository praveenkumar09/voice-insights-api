package com.aia.voiceinsights.api.model;

/** Lightweight run reference for admin listings — full detail comes from GET /api/recommendations/{runId}. */
public record RecommendationRunSummary(String runId, String status, String startedAt) {}
