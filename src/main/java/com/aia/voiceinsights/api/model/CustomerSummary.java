package com.aia.voiceinsights.api.model;

/** One row of the admin customer list — a profile plus its most recent recommendation run, if any. */
public record CustomerSummary(CustomerProfile profile, String latestRunId, String latestRunStatus) {}
