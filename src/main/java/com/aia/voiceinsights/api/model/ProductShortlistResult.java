package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

/** Customer Product Agent — "Which products should we evaluate further for them?" */
public record ProductShortlistResult(
    List<String> shortlistedProducts,
    String rationale
) implements Serializable {}
