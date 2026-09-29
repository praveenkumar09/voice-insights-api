package com.aia.voiceinsights.api.model;

import java.util.List;

/** Live advisor-copilot snapshot, recomputed after every finalized transcript segment and pushed over /ws/voice. */
public record CopilotInsights(
    List<NeedTag> needs,
    Sentiment sentiment,
    BuyingSignal buyingSignal,
    List<String> nextQuestions,
    List<ComplianceFlag> complianceFlags,
    List<ProductMatch> productMatches
) {
    /** strength 0-100 — how strongly the customer has signalled this need. */
    public record NeedTag(String label, int strength) {}

    /** score -100 (very negative) .. +100 (very positive). */
    public record Sentiment(int score, String label, String emotion) {}

    /** score 0-100; level is Cold / Warm / Hot; signals are short paraphrased triggers. */
    public record BuyingSignal(int score, String level, List<String> signals) {}

    /** severity is "high" or "medium". */
    public record ComplianceFlag(String severity, String statement, String advice) {}

    /** fitScore 0-100, evidence is a short excerpt from the product documents. */
    public record ProductMatch(String productName, int fitScore, String evidence, String source) {}
}
