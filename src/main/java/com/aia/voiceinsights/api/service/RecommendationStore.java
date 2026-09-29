package com.aia.voiceinsights.api.service;

import com.aia.voiceinsights.api.model.AgentStepView;
import com.aia.voiceinsights.api.model.RecommendationRunView;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Self-provisioned persistence for recommendation runs — every pipeline
 * step's INPUT and OUTPUT is stored (see {@link #saveAgentStep}), not just
 * its output, so a run stays fully auditable after the fact: what exactly
 * was each agent given, and what did it decide. One generic table for every
 * step (need, risk, affordability, merge, persona, productScoring,
 * productShortlist, ragValidation, summary, complianceCheck, salesReport)
 * rather than a bespoke column/table per agent — the pipeline is expected to
 * keep growing, and a fixed schema per agent doesn't scale with that.
 */
@Service
public class RecommendationStore {

    /** Canonical pipeline order — drives the ordering of GET /api/recommendations/{runId}'s step list. */
    public static final List<String> PIPELINE_ORDER = List.of(
            "need", "risk", "affordability", "merge",
            "persona", "productScoring", "productShortlist",
            "ragValidation", "complianceCheck", "summary", "salesReport"
    );

    private final JdbcTemplate jdbc;
    private final String schema;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    public RecommendationStore(JdbcTemplate jdbc, @Value("${voice.db.schema:voice_insights}") String schema) {
        this.jdbc = jdbc;
        this.schema = schema;
    }

    @PostConstruct
    public void init() {
        jdbc.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS %s.recommendation_runs (
                    run_id              TEXT PRIMARY KEY,
                    customer_profile_id TEXT NOT NULL,
                    status              TEXT NOT NULL,
                    error_message       TEXT,
                    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                    completed_at        TIMESTAMPTZ
                )
                """.formatted(schema));
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS %s.recommendation_agent_results (
                    run_id       TEXT NOT NULL,
                    agent_name   TEXT NOT NULL,
                    status       TEXT NOT NULL,
                    input_json   JSONB,
                    result_json  JSONB,
                    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    completed_at TIMESTAMPTZ,
                    PRIMARY KEY (run_id, agent_name)
                )
                """.formatted(schema));
        // Migration for databases provisioned before input_json existed —
        // CREATE TABLE IF NOT EXISTS above is a no-op against an existing table.
        jdbc.execute("ALTER TABLE %s.recommendation_agent_results ADD COLUMN IF NOT EXISTS input_json JSONB".formatted(schema));
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_vi_runs_profile ON %s.recommendation_runs (customer_profile_id)".formatted(schema));
    }

    /** Most recent run for a customer, if any — drives the admin list's "View Recommendation" button. */
    public Optional<com.aia.voiceinsights.api.model.RecommendationRunSummary> findLatestRun(String customerProfileId) {
        List<com.aia.voiceinsights.api.model.RecommendationRunSummary> rows = jdbc.query(
                """
                SELECT run_id, status, started_at::text FROM %s.recommendation_runs
                WHERE customer_profile_id = ? ORDER BY started_at DESC LIMIT 1
                """.formatted(schema),
                (rs, n) -> new com.aia.voiceinsights.api.model.RecommendationRunSummary(rs.getString(1), rs.getString(2), rs.getString(3)),
                customerProfileId);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public void createRun(String runId, String customerProfileId) {
        jdbc.update("INSERT INTO %s.recommendation_runs (run_id, customer_profile_id, status) VALUES (?, ?, 'RUNNING')"
                .formatted(schema), runId, customerProfileId);
    }

    public void markRunStatus(String runId, String status, String errorMessage) {
        jdbc.update("""
                UPDATE %s.recommendation_runs SET status = ?, error_message = ?, completed_at = now() WHERE run_id = ?
                """.formatted(schema), status, errorMessage, runId);
    }

    /** Persists both what an agent step was given and what it produced — the full audit record for that step. */
    public void saveAgentStep(String runId, String agentKey, String status, Object input, Object output) {
        try {
            String inputJson = input == null ? null : mapper.writeValueAsString(input);
            String outputJson = output == null ? null : mapper.writeValueAsString(output);
            jdbc.update("""
                    INSERT INTO %s.recommendation_agent_results (run_id, agent_name, status, input_json, result_json, completed_at)
                    VALUES (?, ?, ?, ?::jsonb, ?::jsonb, now())
                    ON CONFLICT (run_id, agent_name) DO UPDATE SET
                        status = EXCLUDED.status, input_json = EXCLUDED.input_json,
                        result_json = EXCLUDED.result_json, completed_at = EXCLUDED.completed_at
                    """.formatted(schema), runId, agentKey, status, inputJson, outputJson);
        } catch (Exception e) {
            throw new RuntimeException("Failed to persist recommendation step " + agentKey, e);
        }
    }

    public Optional<RecommendationRunView> getRunView(String runId) {
        List<Map<String, Object>> runRows = jdbc.queryForList(
                "SELECT status, error_message, customer_profile_id FROM %s.recommendation_runs WHERE run_id = ?".formatted(schema), runId);
        if (runRows.isEmpty()) return Optional.empty();

        String status = (String) runRows.get(0).get("status");
        String errorMessage = (String) runRows.get(0).get("error_message");
        String customerProfileId = (String) runRows.get(0).get("customer_profile_id");

        record StepRow(String agentName, String status, String inputJson, String resultJson) {}
        List<StepRow> rows = jdbc.query(
                "SELECT agent_name, status, input_json::text, result_json::text FROM %s.recommendation_agent_results WHERE run_id = ?"
                        .formatted(schema),
                (rs, n) -> new StepRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)),
                runId);

        Map<String, StepRow> byAgent = new LinkedHashMap<>();
        for (StepRow row : rows) byAgent.put(row.agentName(), row);

        List<AgentStepView> steps = new ArrayList<>();
        for (String agentKey : PIPELINE_ORDER) {
            StepRow row = byAgent.get(agentKey);
            if (row == null) continue; // not reached yet (or run failed before this step)
            steps.add(new AgentStepView(agentKey, row.status(), readJsonTree(row.inputJson()), readJsonTree(row.resultJson())));
        }

        return Optional.of(new RecommendationRunView(runId, customerProfileId, status, steps, errorMessage));
    }

    private Object readJsonTree(String json) {
        if (json == null) return null;
        try {
            return mapper.readValue(json, Object.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize stored JSON", e);
        }
    }

    /** Typed accessor for the final report — backs the report download endpoint. */
    public Optional<com.aia.voiceinsights.api.model.SalesReportResult> getSalesReport(String runId) {
        List<String> rows = jdbc.query(
                "SELECT result_json::text FROM %s.recommendation_agent_results WHERE run_id = ? AND agent_name = 'salesReport'"
                        .formatted(schema),
                (rs, n) -> rs.getString(1), runId);
        if (rows.isEmpty() || rows.get(0) == null) return Optional.empty();
        try {
            return Optional.of(mapper.readValue(rows.get(0), com.aia.voiceinsights.api.model.SalesReportResult.class));
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize sales report for run " + runId, e);
        }
    }
}
