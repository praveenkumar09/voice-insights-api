package com.aia.voiceinsights.api.controller;

import com.aia.voiceinsights.api.model.CustomerProfile;
import com.aia.voiceinsights.api.service.AuthStore;
import com.aia.voiceinsights.api.service.CustomerProfileStore;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/customers")
public class CustomerController {

    private final CustomerProfileStore profileStore;
    private final AuthStore authStore;

    public CustomerController(CustomerProfileStore profileStore, AuthStore authStore) {
        this.profileStore = profileStore;
        this.authStore = authStore;
    }

    /**
     * Creates a new profile, or finalizes/updates one already captured by the
     * voice session (when {@code id} is present in the body) — e.g. the last
     * step of the voice-capture flow, or a manual profile for testing the
     * recommendation graph directly.
     */
    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<CustomerProfile> upsert(@RequestBody CustomerProfile profile,
                                                    @RequestHeader(value = "X-Session-Token", required = false) String token) {
        authStore.resolveUserId(token).ifPresent(profile::setAgentUserId);
        // Live insights are produced by the voice session, not by clients — always keep the stored copy.
        profile.setLiveInsights(profile.getId() == null ? null
                : profileStore.findById(profile.getId()).map(CustomerProfile::getLiveInsights).orElse(null));
        if (profile.getStatus() == null || profile.getStatus().isBlank()) {
            profile.setStatus("FINALIZED");
        }
        return ResponseEntity.ok(profileStore.save(profile));
    }

    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> get(@PathVariable String id) {
        return profileStore.findById(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
