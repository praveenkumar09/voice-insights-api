package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

public record ProductScore(
    String productName,
    int score, // 0-100
    List<String> matchReasons,
    List<String> concerns
) implements Serializable {}
