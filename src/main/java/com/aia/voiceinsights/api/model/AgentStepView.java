package com.aia.voiceinsights.api.model;

/**
 * One pipeline step's audit record — both what it was given (input) and what
 * it produced (output), generic (not typed to a specific agent's result
 * class) since a run view spans an open-ended, growing pipeline. input/output
 * are deserialized as plain Map/List/primitive trees (Jackson's default
 * Object mapping), which is all a view endpoint needs — the strongly-typed
 * records are still used everywhere the agent logic itself constructs a
 * result (see RecommendationAgentService), just not on this read side.
 */
public record AgentStepView(String agentKey, String status, Object input, Object output) {}
