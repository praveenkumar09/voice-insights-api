package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

/** Recommendation Summary Agent — "How do I explain this recommendation?" Agent-friendly, customer-facing language. */
public record RecommendationSummaryResult(
    String customerFacingSummary,
    List<String> keyTalkingPoints
) implements Serializable {}
