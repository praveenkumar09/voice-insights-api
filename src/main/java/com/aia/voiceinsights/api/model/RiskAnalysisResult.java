package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

/** Serializable: LangGraph4j clones per-node state via Java serialization (see CompiledGraph.cloneState). */
public record RiskAnalysisResult(
    List<String> riskFactors,
    String riskLevel,
    String rationale
) implements Serializable {}
