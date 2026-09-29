package com.aia.voiceinsights.api.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A customer profile built live from a voice conversation: {@link
 * com.aia.voiceinsights.api.service.ProfileExtractionService} re-derives
 * these fields from the running transcript as the agent talks, so this stays
 * a plain mutable bean (not a record) — both the voice websocket handler and
 * a manual POST /api/customers can set it field-by-field.
 */
public class CustomerProfile {

    private String id;
    private String agentUserId;
    private String status = "IN_PROGRESS"; // IN_PROGRESS | FINALIZED

    private String customerName;
    private Integer age;
    private String occupation;
    private String incomeBand;
    private Integer dependents;
    private List<String> existingPolicies = new ArrayList<>();
    private List<String> goalsAndConcerns = new ArrayList<>();
    private String budgetNotes;
    private String notes;

    private String rawTranscript;

    private Instant createdAt;
    private Instant updatedAt;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getAgentUserId() { return agentUserId; }
    public void setAgentUserId(String agentUserId) { this.agentUserId = agentUserId; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }

    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }

    public String getOccupation() { return occupation; }
    public void setOccupation(String occupation) { this.occupation = occupation; }

    public String getIncomeBand() { return incomeBand; }
    public void setIncomeBand(String incomeBand) { this.incomeBand = incomeBand; }

    public Integer getDependents() { return dependents; }
    public void setDependents(Integer dependents) { this.dependents = dependents; }

    public List<String> getExistingPolicies() { return existingPolicies; }
    public void setExistingPolicies(List<String> existingPolicies) { this.existingPolicies = existingPolicies; }

    public List<String> getGoalsAndConcerns() { return goalsAndConcerns; }
    public void setGoalsAndConcerns(List<String> goalsAndConcerns) { this.goalsAndConcerns = goalsAndConcerns; }

    public String getBudgetNotes() { return budgetNotes; }
    public void setBudgetNotes(String budgetNotes) { this.budgetNotes = budgetNotes; }

    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }

    public String getRawTranscript() { return rawTranscript; }
    public void setRawTranscript(String rawTranscript) { this.rawTranscript = rawTranscript; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
