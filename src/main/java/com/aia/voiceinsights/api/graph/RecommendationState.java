package com.aia.voiceinsights.api.graph;

import org.bsc.langgraph4j.state.AgentState;

import java.util.Map;

/**
 * State keys written by the recommendation graph's nodes: "needResult",
 * "riskResult", "affordabilityResult" (one each, written by the three
 * parallel branches — see RecommendationGraphFactory) and "mergedInsights"
 * (written once by the join/merge node). Every key is written by exactly one
 * node, so no custom {@link org.bsc.langgraph4j.state.Channel} reducers are
 * needed — the default last-write-wins merge is already correct.
 */
public class RecommendationState extends AgentState {
    public RecommendationState(Map<String, Object> initData) {
        super(initData);
    }
}
