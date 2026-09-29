package com.aia.voiceinsights.api.model;

import java.io.Serializable;
import java.util.List;

/** Compliance Check Agent — "Is this recommendation compliant?" Automated suitability review: affordability, eligibility, need match. */
public record ComplianceCheckResult(
    boolean compliant,
    List<ComplianceCheckItem> checks,
    List<String> issues,
    String rationale
) implements Serializable {}
