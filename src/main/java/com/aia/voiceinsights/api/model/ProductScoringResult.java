package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

/** Product Scoring Agent — "Which products we have are their best fit?" Every catalog product, scored, best-first. */
public record ProductScoringResult(
    List<ProductScore> scores,
    String methodology
) implements Serializable {}
