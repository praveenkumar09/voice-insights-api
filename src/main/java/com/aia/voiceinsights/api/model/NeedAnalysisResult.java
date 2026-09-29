package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

/** Serializable: LangGraph4j clones per-node state via Java serialization (see CompiledGraph.cloneState). */
public record NeedAnalysisResult(
    List<String> protectionGaps,
    List<String> recommendedCategories,
    List<String> matchedProductNames,
    String rationale
) implements Serializable {}
