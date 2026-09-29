package com.aia.voiceinsights.api.model;

import java.io.Serializable;

public record EvidenceCitation(
    String productName,
    String docCategory,
    String sourceFile,
    String excerpt
) implements Serializable {}
