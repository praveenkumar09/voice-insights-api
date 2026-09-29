package com.aia.voiceinsights.api.model;

import java.io.Serializable;

/** Serializable: LangGraph4j clones per-node state via Java serialization (see CompiledGraph.cloneState). */
public record MergedInsights(
    NeedAnalysisResult needs,
    RiskAnalysisResult risks,
    AffordabilityResult affordability,
    String combinedNarrative
) implements Serializable {}
