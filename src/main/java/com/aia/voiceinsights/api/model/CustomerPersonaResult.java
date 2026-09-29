package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

/** Customer Persona Agent — "What type of customer is this?" */
public record CustomerPersonaResult(
    String personaLabel,
    String lifeStage,
    List<String> characteristics,
    String rationale
) implements Serializable {}
