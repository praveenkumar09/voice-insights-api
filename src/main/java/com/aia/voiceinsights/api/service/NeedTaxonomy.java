package com.aia.voiceinsights.api.service;

import java.util.List;

/**
 * The one shared list of need categories — used by both the live copilot
 * (needs shown while the customer is talking) and the Need Agent (final
 * analysis), so the UI can compare "live" and "final" by name instead of
 * guessing that "Health cover" and "Medical" mean the same thing.
 */
public final class NeedTaxonomy {

    public static final List<String> CATEGORIES = List.of(
            "Family protection", "Income protection", "Medical", "Critical illness", "Accident cover",
            "Retirement", "Education savings", "Wealth accumulation", "Legacy planning");

    private NeedTaxonomy() {}

    public static String asPromptList() {
        return String.join(", ", CATEGORIES);
    }
}
