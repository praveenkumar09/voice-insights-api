package com.aia.voiceinsights.api.model;

public record ProductChunkMatch(
    String chunkId,
    String productName,
    String docCategory,
    String sourceFile,
    String sectionTitle,
    String content,
    double similarity
) {}
