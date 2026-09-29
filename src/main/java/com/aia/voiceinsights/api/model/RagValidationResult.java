package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

/** RAG Validation Agent — "What evidence supports this recommendation?" Grounded in the ingested product documents. */
public record RagValidationResult(
    List<EvidenceCitation> citations,
    boolean allClaimsSupported,
    String notes
) implements Serializable {}
