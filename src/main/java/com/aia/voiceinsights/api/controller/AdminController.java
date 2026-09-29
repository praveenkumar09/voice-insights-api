package com.aia.voiceinsights.api.controller;

import com.aia.voiceinsights.api.model.CustomerProfile;
import com.aia.voiceinsights.api.model.CustomerSummary;
import com.aia.voiceinsights.api.model.PageResult;
import com.aia.voiceinsights.api.service.CustomerProfileStore;
import com.aia.voiceinsights.api.service.RecommendationStore;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only admin views over everything captured by the voice/recommendation
 * pipeline — backs voice-insights-ui's admin page. Every voice session and
 * recommendation run is already persisted (CustomerProfileStore,
 * RecommendationStore, both in this service's own {@code voice_insights}
 * Postgres schema); this just adds pagination and a per-customer "latest run"
 * lookup on top of storage that already existed.
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private static final int MAX_PAGE_SIZE = 100;

    private final CustomerProfileStore profileStore;
    private final RecommendationStore recommendationStore;

    public AdminController(CustomerProfileStore profileStore, RecommendationStore recommendationStore) {
        this.profileStore = profileStore;
        this.recommendationStore = recommendationStore;
    }

    @GetMapping(value = "/customers", produces = MediaType.APPLICATION_JSON_VALUE)
    public PageResult<CustomerSummary> listCustomers(@RequestParam(defaultValue = "0") int page,
                                                       @RequestParam(defaultValue = "20") int size) {
        int safeSize = Math.max(1, Math.min(size, MAX_PAGE_SIZE));
        int safePage = Math.max(0, page);

        var items = profileStore.findAll(safePage, safeSize).stream()
                .map(this::toSummary)
                .toList();
        long total = profileStore.count();

        return new PageResult<>(items, safePage, safeSize, total);
    }

    private CustomerSummary toSummary(CustomerProfile profile) {
        return recommendationStore.findLatestRun(profile.getId())
                .map(run -> new CustomerSummary(profile, run.runId(), run.status()))
                .orElseGet(() -> new CustomerSummary(profile, null, null));
    }
}
