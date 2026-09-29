package com.aia.voiceinsights.api.graph;

import com.aia.voiceinsights.api.model.*;
import com.aia.voiceinsights.api.service.RecommendationAgentService;
import com.aia.voiceinsights.api.service.RecommendationEventBus;
import com.aia.voiceinsights.api.service.RecommendationStore;
import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphStateException;
import org.bsc.langgraph4j.StateGraph;
import org.bsc.langgraph4j.action.AsyncNodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Builds the per-run LangGraph4j graph for the full AIA Singapore advisory
 * pipeline: three parallel analysis agents fan out from START, converge on a
 * merge/synthesis node, and then flow through a sequential chain of six more
 * agents down to a final downloadable sales report.
 *
 * <pre>
 *         START
 *           |
 *    +------+------+
 *    v      v      v
 *  need   risk  affordability
 *    +------+------+
 *           v
 *         merge
 *           v
 *        persona
 *           v
 *    productScoring
 *           v
 *   productShortlist
 *           v
 *     ragValidation
 *           v
 *    complianceCheck
 *           v
 *         summary
 *           v
 *      salesReport
 *           v
 *          END
 * </pre>
 *
 * Compliance runs before the customer-facing summary is drafted — the
 * summary agent is given the compliance verdict and writes a caveated
 * explanation when the recommendation didn't clear compliance, rather than
 * pitching a recommendation that compliance goes on to reject.
 *
 * Every node publishes started/completed/failed events to {@link
 * RecommendationEventBus} (driving the frontend's live pipeline animation)
 * and persists BOTH its input and its output via {@link
 * RecommendationStore#saveAgentStep} — a full audit trail of what every agent
 * was given and what it decided, not just the final answer. A node never
 * lets an LLM/parsing failure abort the whole graph: on error it publishes
 * agent_failed and still returns a (marked) fallback result, so downstream
 * steps still run against a well-formed (if degraded) input instead of the
 * whole pipeline dying on one bad call.
 */
@Component
public class RecommendationGraphFactory {

    private static final Logger log = LoggerFactory.getLogger(RecommendationGraphFactory.class);

    private final RecommendationAgentService agentService;
    private final RecommendationStore store;
    private final RecommendationEventBus eventBus;

    public RecommendationGraphFactory(RecommendationAgentService agentService,
                                       RecommendationStore store,
                                       RecommendationEventBus eventBus) {
        this.agentService = agentService;
        this.store = store;
        this.eventBus = eventBus;
    }

    public CompiledGraph<RecommendationState> build(String runId, CustomerProfile profile) throws GraphStateException {
        StateGraph<RecommendationState> graph = new StateGraph<>(RecommendationState::new);

        graph.addNode("need", AsyncNodeAction.node_async(state -> runNeed(runId, profile)));
        graph.addNode("risk", AsyncNodeAction.node_async(state -> runRisk(runId, profile)));
        graph.addNode("affordability", AsyncNodeAction.node_async(state -> runAffordability(runId, profile)));
        graph.addNode("merge", AsyncNodeAction.node_async(state -> runMerge(runId, profile, state)));
        graph.addNode("persona", AsyncNodeAction.node_async(state -> runPersona(runId, profile, state)));
        graph.addNode("productScoring", AsyncNodeAction.node_async(state -> runProductScoring(runId, profile, state)));
        graph.addNode("productShortlist", AsyncNodeAction.node_async(state -> runProductShortlist(runId, state)));
        graph.addNode("ragValidation", AsyncNodeAction.node_async(state -> runRagValidation(runId, state)));
        graph.addNode("summary", AsyncNodeAction.node_async(state -> runSummary(runId, profile, state)));
        graph.addNode("complianceCheck", AsyncNodeAction.node_async(state -> runComplianceCheck(runId, profile, state)));
        graph.addNode("salesReport", AsyncNodeAction.node_async(state -> runSalesReport(runId, profile, state)));

        graph.addEdge(StateGraph.START, "need");
        graph.addEdge(StateGraph.START, "risk");
        graph.addEdge(StateGraph.START, "affordability");
        graph.addEdge("need", "merge");
        graph.addEdge("risk", "merge");
        graph.addEdge("affordability", "merge");
        graph.addEdge("merge", "persona");
        graph.addEdge("persona", "productScoring");
        graph.addEdge("productScoring", "productShortlist");
        graph.addEdge("productShortlist", "ragValidation");
        graph.addEdge("ragValidation", "complianceCheck");
        graph.addEdge("complianceCheck", "summary");
        graph.addEdge("summary", "salesReport");
        graph.addEdge("salesReport", StateGraph.END);

        return graph.compile();
    }

    // ── Stage 1: parallel analysis ──────────────────────────────────────────

    private Map<String, Object> runNeed(String runId, CustomerProfile profile) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("need"));
        Instant start = Instant.now();
        try {
            NeedAnalysisResult result = agentService.analyzeNeed(profile);
            store.saveAgentStep(runId, "need", "COMPLETED", profile, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("need", result));
            logCompleted("need", runId, start);
            return Map.of("needResult", result);
        } catch (Exception e) {
            NeedAnalysisResult fallback = new NeedAnalysisResult(List.of(), List.of(), List.of(),
                    "Need analysis failed: " + e.getMessage());
            store.saveAgentStep(runId, "need", "FAILED", profile, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("need", e.getMessage()));
            logFailed("need", runId, start, e);
            return Map.of("needResult", fallback);
        }
    }

    private Map<String, Object> runRisk(String runId, CustomerProfile profile) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("risk"));
        Instant start = Instant.now();
        try {
            RiskAnalysisResult result = agentService.analyzeRisk(profile);
            store.saveAgentStep(runId, "risk", "COMPLETED", profile, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("risk", result));
            logCompleted("risk", runId, start);
            return Map.of("riskResult", result);
        } catch (Exception e) {
            RiskAnalysisResult fallback = new RiskAnalysisResult(List.of(), "Unknown",
                    "Risk analysis failed: " + e.getMessage());
            store.saveAgentStep(runId, "risk", "FAILED", profile, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("risk", e.getMessage()));
            logFailed("risk", runId, start, e);
            return Map.of("riskResult", fallback);
        }
    }

    private Map<String, Object> runAffordability(String runId, CustomerProfile profile) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("affordability"));
        Instant start = Instant.now();
        try {
            AffordabilityResult result = agentService.analyzeAffordability(profile);
            store.saveAgentStep(runId, "affordability", "COMPLETED", profile, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("affordability", result));
            logCompleted("affordability", runId, start);
            return Map.of("affordabilityResult", result);
        } catch (Exception e) {
            AffordabilityResult fallback = new AffordabilityResult("Unknown", "Insufficient information",
                    "Affordability analysis failed: " + e.getMessage());
            store.saveAgentStep(runId, "affordability", "FAILED", profile, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("affordability", e.getMessage()));
            logFailed("affordability", runId, start, e);
            return Map.of("affordabilityResult", fallback);
        }
    }

    // ── Stage 2: synthesis ───────────────────────────────────────────────────

    private Map<String, Object> runMerge(String runId, CustomerProfile profile, RecommendationState state) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("merge"));
        Instant start = Instant.now();

        NeedAnalysisResult need = state.<NeedAnalysisResult>value("needResult")
                .orElseGet(() -> new NeedAnalysisResult(List.of(), List.of(), List.of(), "Not available"));
        RiskAnalysisResult risk = state.<RiskAnalysisResult>value("riskResult")
                .orElseGet(() -> new RiskAnalysisResult(List.of(), "Unknown", "Not available"));
        AffordabilityResult affordability = state.<AffordabilityResult>value("affordabilityResult")
                .orElseGet(() -> new AffordabilityResult("Unknown", "Not available", "Not available"));
        Map<String, Object> input = Map.of("need", need, "risk", risk, "affordability", affordability);

        try {
            MergedInsights merged = agentService.merge(profile, need, risk, affordability);
            store.saveAgentStep(runId, "merge", "COMPLETED", input, merged);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("merge", merged));
            logCompleted("merge", runId, start);
            return Map.of("mergedInsights", merged);
        } catch (Exception e) {
            MergedInsights fallback = new MergedInsights(need, risk, affordability,
                    "Unable to synthesize a combined narrative: " + e.getMessage());
            store.saveAgentStep(runId, "merge", "FAILED", input, fallback);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("merge", fallback));
            logFailed("merge", runId, start, e);
            return Map.of("mergedInsights", fallback);
        }
    }

    // ── Stage 3: persona ─────────────────────────────────────────────────────

    private Map<String, Object> runPersona(String runId, CustomerProfile profile, RecommendationState state) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("persona"));
        Instant start = Instant.now();
        MergedInsights merged = requireMerged(state);
        Map<String, Object> input = Map.of("profile", profile, "mergedInsights", merged);

        try {
            CustomerPersonaResult result = agentService.buildPersona(profile, merged);
            store.saveAgentStep(runId, "persona", "COMPLETED", input, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("persona", result));
            logCompleted("persona", runId, start);
            return Map.of("personaResult", result);
        } catch (Exception e) {
            CustomerPersonaResult fallback = new CustomerPersonaResult("Unclassified", "Unknown", List.of(),
                    "Persona classification failed: " + e.getMessage());
            store.saveAgentStep(runId, "persona", "FAILED", input, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("persona", e.getMessage()));
            logFailed("persona", runId, start, e);
            return Map.of("personaResult", fallback);
        }
    }

    // ── Stage 4: product scoring ─────────────────────────────────────────────

    private Map<String, Object> runProductScoring(String runId, CustomerProfile profile, RecommendationState state) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("productScoring"));
        Instant start = Instant.now();
        MergedInsights merged = requireMerged(state);
        CustomerPersonaResult persona = state.<CustomerPersonaResult>value("personaResult")
                .orElseGet(() -> new CustomerPersonaResult("Unclassified", "Unknown", List.of(), "Not available"));
        Map<String, Object> input = Map.of("profile", profile, "persona", persona, "mergedInsights", merged);

        try {
            ProductScoringResult result = agentService.scoreProducts(profile, merged, persona);
            store.saveAgentStep(runId, "productScoring", "COMPLETED", input, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("productScoring", result));
            logCompleted("productScoring", runId, start);
            return Map.of("scoringResult", result);
        } catch (Exception e) {
            ProductScoringResult fallback = new ProductScoringResult(List.of(), "Scoring failed: " + e.getMessage());
            store.saveAgentStep(runId, "productScoring", "FAILED", input, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("productScoring", e.getMessage()));
            logFailed("productScoring", runId, start, e);
            return Map.of("scoringResult", fallback);
        }
    }

    // ── Stage 5: shortlist ────────────────────────────────────────────────────

    private Map<String, Object> runProductShortlist(String runId, RecommendationState state) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("productShortlist"));
        Instant start = Instant.now();
        ProductScoringResult scoring = state.<ProductScoringResult>value("scoringResult")
                .orElseGet(() -> new ProductScoringResult(List.of(), "Not available"));

        try {
            ProductShortlistResult result = agentService.shortlistProducts(scoring);
            store.saveAgentStep(runId, "productShortlist", "COMPLETED", scoring, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("productShortlist", result));
            logCompleted("productShortlist", runId, start);
            return Map.of("shortlistResult", result);
        } catch (Exception e) {
            ProductShortlistResult fallback = new ProductShortlistResult(List.of(), "Shortlisting failed: " + e.getMessage());
            store.saveAgentStep(runId, "productShortlist", "FAILED", scoring, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("productShortlist", e.getMessage()));
            logFailed("productShortlist", runId, start, e);
            return Map.of("shortlistResult", fallback);
        }
    }

    // ── Stage 6: RAG validation ──────────────────────────────────────────────

    private Map<String, Object> runRagValidation(String runId, RecommendationState state) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("ragValidation"));
        Instant start = Instant.now();
        MergedInsights merged = requireMerged(state);
        ProductShortlistResult shortlist = requireShortlist(state);
        Map<String, Object> input = Map.of("mergedInsights", merged, "shortlist", shortlist);

        try {
            RagValidationResult result = agentService.validateWithRag(merged, shortlist);
            store.saveAgentStep(runId, "ragValidation", "COMPLETED", input, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("ragValidation", result));
            logCompleted("ragValidation", runId, start);
            return Map.of("validationResult", result);
        } catch (Exception e) {
            RagValidationResult fallback = new RagValidationResult(List.of(), false, "Validation failed: " + e.getMessage());
            store.saveAgentStep(runId, "ragValidation", "FAILED", input, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("ragValidation", e.getMessage()));
            logFailed("ragValidation", runId, start, e);
            return Map.of("validationResult", fallback);
        }
    }

    // ── Stage 7: compliance check ─────────────────────────────────────────────

    private Map<String, Object> runComplianceCheck(String runId, CustomerProfile profile, RecommendationState state) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("complianceCheck"));
        Instant start = Instant.now();
        MergedInsights merged = requireMerged(state);
        ProductShortlistResult shortlist = requireShortlist(state);
        RagValidationResult validation = requireValidation(state);
        Map<String, Object> input = Map.of("profile", profile, "mergedInsights", merged,
                "shortlist", shortlist, "validation", validation);

        try {
            ComplianceCheckResult result = agentService.checkCompliance(profile, merged, shortlist, validation);
            store.saveAgentStep(runId, "complianceCheck", "COMPLETED", input, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("complianceCheck", result));
            logCompleted("complianceCheck", runId, start);
            return Map.of("complianceResult", result);
        } catch (Exception e) {
            ComplianceCheckResult fallback = new ComplianceCheckResult(false, List.of(),
                    List.of("Compliance check could not be completed: " + e.getMessage()), "Automated check failed.");
            store.saveAgentStep(runId, "complianceCheck", "FAILED", input, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("complianceCheck", e.getMessage()));
            logFailed("complianceCheck", runId, start, e);
            return Map.of("complianceResult", fallback);
        }
    }

    // ── Stage 8: customer-facing summary ─────────────────────────────────────
    // Runs after compliance so the pitch can be written with the compliance
    // verdict already known — see RecommendationAgentService.summarize's Javadoc.

    private Map<String, Object> runSummary(String runId, CustomerProfile profile, RecommendationState state) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("summary"));
        Instant start = Instant.now();
        MergedInsights merged = requireMerged(state);
        ProductShortlistResult shortlist = requireShortlist(state);
        RagValidationResult validation = requireValidation(state);
        ComplianceCheckResult compliance = state.<ComplianceCheckResult>value("complianceResult")
                .orElseGet(() -> new ComplianceCheckResult(false, List.of(), List.of(), "Not available"));
        Map<String, Object> input = Map.of("profile", profile, "mergedInsights", merged,
                "shortlist", shortlist, "validation", validation, "compliance", compliance);

        try {
            RecommendationSummaryResult result = agentService.summarize(profile, merged, shortlist, validation, compliance);
            store.saveAgentStep(runId, "summary", "COMPLETED", input, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("summary", result));
            logCompleted("summary", runId, start);
            return Map.of("summaryResult", result);
        } catch (Exception e) {
            RecommendationSummaryResult fallback = new RecommendationSummaryResult(
                    "Unable to generate a customer-facing summary: " + e.getMessage(), List.of());
            store.saveAgentStep(runId, "summary", "FAILED", input, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("summary", e.getMessage()));
            logFailed("summary", runId, start, e);
            return Map.of("summaryResult", fallback);
        }
    }

    // ── Stage 9: sales report ─────────────────────────────────────────────────

    private Map<String, Object> runSalesReport(String runId, CustomerProfile profile, RecommendationState state) {
        eventBus.publish(runId, RecommendationEvent.agentStarted("salesReport"));
        Instant start = Instant.now();
        MergedInsights merged = requireMerged(state);
        CustomerPersonaResult persona = state.<CustomerPersonaResult>value("personaResult")
                .orElseGet(() -> new CustomerPersonaResult("Unclassified", "Unknown", List.of(), "Not available"));
        ProductScoringResult scoring = state.<ProductScoringResult>value("scoringResult")
                .orElseGet(() -> new ProductScoringResult(List.of(), "Not available"));
        ProductShortlistResult shortlist = requireShortlist(state);
        RagValidationResult validation = requireValidation(state);
        RecommendationSummaryResult summary = state.<RecommendationSummaryResult>value("summaryResult")
                .orElseGet(() -> new RecommendationSummaryResult("Not available", List.of()));
        ComplianceCheckResult compliance = state.<ComplianceCheckResult>value("complianceResult")
                .orElseGet(() -> new ComplianceCheckResult(false, List.of(), List.of(), "Not available"));

        Map<String, Object> input = Map.of(
                "profile", profile, "mergedInsights", merged, "persona", persona, "scoring", scoring,
                "shortlist", shortlist, "validation", validation, "summary", summary, "compliance", compliance);

        try {
            SalesReportResult result = agentService.generateSalesReport(
                    profile, merged, persona, scoring, shortlist, validation, summary, compliance);
            store.saveAgentStep(runId, "salesReport", "COMPLETED", input, result);
            eventBus.publish(runId, RecommendationEvent.agentCompleted("salesReport", result));
            logCompleted("salesReport", runId, start);
            return Map.of("salesReportResult", result);
        } catch (Exception e) {
            SalesReportResult fallback = new SalesReportResult("Sales report generation failed",
                    "Report could not be generated: " + e.getMessage());
            store.saveAgentStep(runId, "salesReport", "FAILED", input, fallback);
            eventBus.publish(runId, RecommendationEvent.agentFailed("salesReport", e.getMessage()));
            logFailed("salesReport", runId, start, e);
            return Map.of("salesReportResult", fallback);
        }
    }

    // ── Shared state readers ──────────────────────────────────────────────────

    private MergedInsights requireMerged(RecommendationState state) {
        return state.<MergedInsights>value("mergedInsights").orElseGet(() -> new MergedInsights(
                new NeedAnalysisResult(List.of(), List.of(), List.of(), "Not available"),
                new RiskAnalysisResult(List.of(), "Unknown", "Not available"),
                new AffordabilityResult("Unknown", "Not available", "Not available"),
                "Not available"));
    }

    private ProductShortlistResult requireShortlist(RecommendationState state) {
        return state.<ProductShortlistResult>value("shortlistResult")
                .orElseGet(() -> new ProductShortlistResult(List.of(), "Not available"));
    }

    private RagValidationResult requireValidation(RecommendationState state) {
        return state.<RagValidationResult>value("validationResult")
                .orElseGet(() -> new RagValidationResult(List.of(), false, "Not available"));
    }

    // ── Per-node timing/outcome logging ─────────────────────────────────────

    private void logCompleted(String agentKey, String runId, Instant start) {
        log.info("Agent {} completed for run {} in {} ms", agentKey, runId, Duration.between(start, Instant.now()).toMillis());
    }

    private void logFailed(String agentKey, String runId, Instant start, Exception e) {
        log.warn("Agent {} failed for run {} after {} ms: {}",
                agentKey, runId, Duration.between(start, Instant.now()).toMillis(), e.getMessage(), e);
    }
}
