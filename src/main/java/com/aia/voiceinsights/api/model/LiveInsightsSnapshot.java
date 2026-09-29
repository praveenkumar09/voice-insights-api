package com.aia.voiceinsights.api.model;

import java.util.List;

/**
 * What the live copilot concluded during the call, stored with the customer
 * profile (server-side only) so the analysis agents can use the conversation
 * signals, the UI can compare live vs final, and the audit trail can explain
 * any difference later. {@code latest} is the last (post-call) copilot pass;
 * {@code history} is the sentiment / buying-signal trajectory, one point per update.
 */
public record LiveInsightsSnapshot(CopilotInsights latest, List<SignalPoint> history) {

    public record SignalPoint(int sentiment, int buying) {}
}
