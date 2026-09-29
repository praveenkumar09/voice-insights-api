package com.aia.voiceinsights.api.service;

import com.aia.voiceinsights.api.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.ResponseFormat;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The actual LLM calls (and, for the two steps where determinism matters
 * more than language, the actual selection/assembly logic) behind every
 * LangGraph4j node in the recommendation pipeline — see
 * graph/RecommendationGraphFactory for how these are wired together, and
 * RecommendationStore for how each step's input AND output get persisted for
 * audit. Every LLM call follows the same chatModel.call(Prompt,
 * responseFormat=JSON_OBJECT) + manual deserialization pattern
 * cobalt-rag-api's RerankService/CodeChangeService use, rather than an
 * unverified higher-level structured-output API.
 *
 * Pipeline (matches the product/business spec exactly):
 *   [need | risk | affordability] (parallel) -> merge -> persona ->
 *   productScoring -> productShortlist -> ragValidation -> complianceCheck ->
 *   summary -> salesReport
 *
 * Compliance runs BEFORE the customer-facing summary is drafted, not after —
 * the summary is written to reflect the compliance verdict (softened /
 * caveated language when the recommendation didn't clear compliance) rather
 * than pitching a recommendation that compliance goes on to reject.
 */
@Service
public class RecommendationAgentService {

    private static final Logger log = LoggerFactory.getLogger(RecommendationAgentService.class);

    private static final ResponseFormat JSON_FORMAT =
            ResponseFormat.builder().type(ResponseFormat.Type.JSON_OBJECT).build();

    /** How many products the Customer Product Agent shortlists out of the full scored catalog. */
    private static final int SHORTLIST_SIZE = 3;

    /** How many evidence excerpts the RAG Validation agent pulls per shortlisted product. */
    private static final int EVIDENCE_PER_PRODUCT = 3;

    private static final int EVIDENCE_EXCERPT_MAX_CHARS = 320;

    /** Bounds a single OpenAI call so one hung request can't block a node (and its
     *  orchestration-run thread) forever — see runGraph's un-timed-out predecessor. */
    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(45);

    /** A transient 429/5xx/timeout gets retried instead of immediately falling back —
     *  the fallback path exists for genuine failures, not for one bad network blip. */
    private static final int MAX_ATTEMPTS = 3;
    private static final Duration RETRY_BASE_DELAY = Duration.ofMillis(400);

    private final ChatModel chatModel;
    private final ProductVectorSearchService productSearch;
    private final ObjectMapper mapper = new ObjectMapper();

    /** Bounded, dedicated to running/timing-out LLM calls — separate from the
     *  per-run executor in RecommendationOrchestrationService, since several
     *  agents within one run (and several concurrent runs) call the model at once. */
    private final ExecutorService llmExecutor = Executors.newFixedThreadPool(24);

    public RecommendationAgentService(ChatModel chatModel, ProductVectorSearchService productSearch) {
        this.chatModel = chatModel;
        this.productSearch = productSearch;
    }

    @PreDestroy
    public void shutdown() {
        llmExecutor.shutdown();
    }

    // ── 1. Need Agent — "What is the customer looking for?" ────────────────

    private static final String NEED_SYSTEM_PROMPT = ("""
            You are the Need Agent in AIA Singapore's voice-driven insurance
            recommendation engine. Purpose: determine the customer's intentions —
            what are they actually looking for, in their own words and context. You
            are given a customer profile captured live during an AIA agent's
            conversation with the customer, plus candidate excerpts from AIA
            Singapore's product catalog retrieved for this customer.

            Identify what protection gap(s) exist and which product categories
            would close them, grounded ONLY in the provided candidate excerpts —
            never invent a product or benefit that isn't in the excerpts.

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            {
              "protectionGaps": ["short phrase per gap identified"],
              "recommendedCategories": ["each MUST start with one of the standard need categories (%s), optionally followed by ' – ' and the product type, e.g. 'Family protection – Term Life'"],
              "matchedProductNames": ["product names pulled from the candidate excerpts that address the gaps"],
              "rationale": "2-4 sentences explaining the reasoning, referencing specific profile facts"
            }
            """).formatted(NeedTaxonomy.asPromptList());

    public NeedAnalysisResult analyzeNeed(CustomerProfile profile) {
        List<ProductChunkMatch> candidates = productSearch.search(profileSearchQuery(profile));
        String userMessage = "Customer profile:\n" + profileSummary(profile)
                + "\n\nCandidate product excerpts:\n" + productCandidatesBlock(candidates);
        return call(NEED_SYSTEM_PROMPT, userMessage, NeedAnalysisResult.class);
    }

    // ── 2. Risk Agent — "What risk is he exposed to?" ───────────────────────

    private static final String RISK_SYSTEM_PROMPT = """
            You are the Risk Agent in AIA Singapore's voice-driven insurance
            recommendation engine. Purpose: identify what could financially affect
            this customer — their potential financial and insurance risk exposure
            (occupation risk, health mentions, dependents, existing coverage gaps,
            lifestyle mentions) — from a customer profile captured live during an
            AIA agent's conversation with the customer.

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            {
              "riskFactors": ["short phrase per risk factor identified"],
              "riskLevel": "Low | Moderate | High",
              "rationale": "2-4 sentences explaining the reasoning, referencing specific profile facts"
            }
            """;

    public RiskAnalysisResult analyzeRisk(CustomerProfile profile) {
        return call(RISK_SYSTEM_PROMPT, "Customer profile:\n" + profileSummary(profile), RiskAnalysisResult.class);
    }

    // ── 3. Affordability Agent — "What can they realistically afford?" ─────

    private static final String AFFORDABILITY_SYSTEM_PROMPT = """
            You are the Affordability Agent in AIA Singapore's voice-driven
            insurance recommendation engine. Purpose: prevent unsuitable
            recommendations by calculating a realistic premium range for this
            customer — grounded BOTH in what they said in conversation (income/
            budget signals in the profile) AND in real premium/pricing information
            from AIA Singapore's product catalog excerpts provided below. If the
            conversation gave no income/budget signal, say so explicitly rather
            than guessing a number, but still use the catalog excerpts to describe
            what a typical entry-level premium looks like for context.

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            {
              "estimatedBudgetBand": "e.g. Low | Moderate | Comfortable | Unknown",
              "affordablePremiumRange": "e.g. SGD 80-150/month, or 'Insufficient information'",
              "rationale": "2-4 sentences explaining the reasoning, referencing specific profile facts and/or catalog premium figures"
            }
            """;

    public AffordabilityResult analyzeAffordability(CustomerProfile profile) {
        List<ProductChunkMatch> premiumContext = productSearch.search(profileSearchQuery(profile) + " premium price illustrative cost");
        String userMessage = "Customer profile:\n" + profileSummary(profile)
                + "\n\nProduct catalog excerpts (premium/pricing context):\n" + productCandidatesBlock(premiumContext);
        return call(AFFORDABILITY_SYSTEM_PROMPT, userMessage, AffordabilityResult.class);
    }

    // ── Merge — synthesis of Need + Risk + Affordability ────────────────────

    private static final String MERGE_SYSTEM_PROMPT = """
            You are the synthesis step of AIA Singapore's insurance recommendation
            workflow. You are given a customer profile and the independent outputs
            of the Need, Risk, and Affordability agents. Combine them into one
            coherent narrative an AIA agent can read aloud to the customer — 3-5
            sentences, plain language, no jargon, reconciling all three where
            relevant (e.g. affordability constraints against recommended categories).

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            { "combinedNarrative": "..." }
            """;

    public MergedInsights merge(CustomerProfile profile, NeedAnalysisResult need, RiskAnalysisResult risk, AffordabilityResult affordability) {
        try {
            String userMessage = "Customer profile:\n" + profileSummary(profile)
                    + "\n\nNeed Analysis: " + mapper.writeValueAsString(need)
                    + "\n\nRisk Analysis: " + mapper.writeValueAsString(risk)
                    + "\n\nAffordability: " + mapper.writeValueAsString(affordability);
            MergeNarrative narrative = call(MERGE_SYSTEM_PROMPT, userMessage, MergeNarrative.class);
            return new MergedInsights(need, risk, affordability, narrative.combinedNarrative());
        } catch (Exception e) {
            throw new RuntimeException("Merge synthesis failed", e);
        }
    }

    private record MergeNarrative(String combinedNarrative) {}

    // ── 4. Customer Persona Agent — "What type of customer is this?" ───────

    private static final String PERSONA_SYSTEM_PROMPT = """
            You are the Customer Persona Agent in AIA Singapore's insurance
            recommendation engine. Purpose: group this customer into a meaningful
            life-stage segment AIA advisors recognize, from their profile and the
            merged Need/Risk/Affordability analysis. Typical AIA Singapore
            life-stage segments include (use these as a guide, not a rigid list):
            Young Professional, Newly Married, Growing Family, Established Family,
            Pre-Retirement, Retiree, Business Owner.

            You may also be given "Conversation signals" (customer sentiment and
            buying-signal trend measured live during the call). Treat them as
            supporting context about tone and readiness only — never let them
            override the profile or the analysis facts.

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            {
              "personaLabel": "short persona name, e.g. 'Growing Family Protector'",
              "lifeStage": "one of the segments above (or a close variant)",
              "characteristics": ["short phrase per defining characteristic"],
              "rationale": "2-3 sentences grounding the classification in specific profile/analysis facts"
            }
            """;

    public CustomerPersonaResult buildPersona(CustomerProfile profile, MergedInsights merged) {
        try {
            String userMessage = "Customer profile:\n" + profileSummary(profile)
                    + signalsBlock(profile)
                    + "\n\nMerged analysis: " + mapper.writeValueAsString(merged);
            return call(PERSONA_SYSTEM_PROMPT, userMessage, CustomerPersonaResult.class);
        } catch (Exception e) {
            throw new RuntimeException("Persona classification failed", e);
        }
    }

    // ── 5. Product Scoring Agent — "Which products are their best fit?" ────

    private static final String SCORING_SYSTEM_PROMPT = """
            You are the Product Scoring Agent in AIA Singapore's insurance
            recommendation engine. Purpose: score EVERY product in the catalog
            below against this customer, to produce an unbiased ranking — not just
            the ones that look like an obvious match. You are given the customer's
            profile, persona, and merged Need/Risk/Affordability analysis, plus an
            overview excerpt of every product AIA Singapore currently offers.

            For EACH product listed, score it 0-100 on overall fit for this specific
            customer (need match, risk coverage, affordability fit), with brief
            match reasons and any concerns. Score every product listed — do not
            skip any, even ones that score low.

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            {
              "scores": [
                {"productName": "...", "score": 0-100, "matchReasons": ["..."], "concerns": ["..."]}
              ],
              "methodology": "1-2 sentences on how scores were weighted (need match vs. risk coverage vs. affordability)"
            }
            """;

    public ProductScoringResult scoreProducts(CustomerProfile profile, MergedInsights merged, CustomerPersonaResult persona) {
        List<String> productNames = productSearch.listProductNames();
        StringBuilder catalogBlock = new StringBuilder();
        for (String name : productNames) {
            catalogBlock.append("### ").append(name).append("\n");
            for (ProductChunkMatch chunk : productSearch.getProductOverview(name)) {
                catalogBlock.append("[").append(chunk.docCategory()).append("] ").append(chunk.content()).append("\n");
            }
            catalogBlock.append("\n");
        }

        try {
            String userMessage = "Customer profile:\n" + profileSummary(profile)
                    + "\n\nPersona: " + mapper.writeValueAsString(persona)
                    + "\n\nMerged analysis: " + mapper.writeValueAsString(merged)
                    + "\n\nFull product catalog (" + productNames.size() + " products):\n" + catalogBlock;
            return call(SCORING_SYSTEM_PROMPT, userMessage, ProductScoringResult.class);
        } catch (Exception e) {
            throw new RuntimeException("Product scoring failed", e);
        }
    }

    // ── 6. Customer Product Agent — shortlist (deterministic) ───────────────

    /**
     * Purpose: reduce a scored catalog of (potentially hundreds of) products
     * down to a short evaluation list. Deliberately deterministic (top-N by
     * the Product Scoring agent's own numeric score) rather than a second LLM
     * call re-picking from the list — a fixed, reproducible selection rule is
     * more reliable here than asking a model to re-read scores it already
     * produced, and it removes a place where the shortlist could silently
     * diverge from the scores that are supposed to justify it.
     */
    public ProductShortlistResult shortlistProducts(ProductScoringResult scoring) {
        List<ProductScore> ranked = scoring.scores().stream()
                .sorted(Comparator.comparingInt(ProductScore::score).reversed())
                .toList();
        List<String> shortlist = ranked.stream().limit(SHORTLIST_SIZE).map(ProductScore::productName).toList();

        StringBuilder rationale = new StringBuilder("Top " + shortlist.size() + " of " + ranked.size()
                + " scored products, ranked by fit score: ");
        for (int i = 0; i < ranked.size() && i < SHORTLIST_SIZE; i++) {
            if (i > 0) rationale.append("; ");
            rationale.append(ranked.get(i).productName()).append(" (").append(ranked.get(i).score()).append("/100)");
        }
        return new ProductShortlistResult(shortlist, rationale.toString());
    }

    // ── 7. RAG Validation Agent — "What evidence supports this?" ────────────

    private static final String RAG_VALIDATION_SYSTEM_PROMPT = """
            You are the RAG Validation Agent in AIA Singapore's insurance
            recommendation engine. Purpose: validate that the shortlisted product
            recommendation is factually correct by checking it against real
            excerpts retrieved from AIA Singapore's own product documents
            (Product Summary, Contract, Fact Sheet, Rider, FAQ). You are given the
            merged customer analysis and the retrieved evidence excerpts per
            shortlisted product below.

            Determine whether the evidence actually supports recommending these
            products for this customer's identified needs. Be honest — if the
            evidence is thin or doesn't clearly support a claim, say so in "notes"
            and set allClaimsSupported to false.

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            { "allClaimsSupported": true|false, "notes": "1-3 sentences on what is/isn't well-evidenced" }
            """;

    public RagValidationResult validateWithRag(MergedInsights merged, ProductShortlistResult shortlist) {
        List<EvidenceCitation> citations = new ArrayList<>();
        String needContext = merged.needs() != null ? String.join(" ", merged.needs().protectionGaps()) : "";

        for (String productName : shortlist.shortlistedProducts()) {
            List<ProductChunkMatch> matches = productSearch.searchWithinProduct(
                    productName, productName + " " + needContext, EVIDENCE_PER_PRODUCT);
            for (ProductChunkMatch m : matches) {
                citations.add(new EvidenceCitation(productName, m.docCategory(), m.sourceFile(), excerpt(m.content())));
            }
        }

        try {
            StringBuilder evidenceBlock = new StringBuilder();
            for (EvidenceCitation c : citations) {
                evidenceBlock.append("- [").append(c.productName()).append(" / ").append(c.docCategory())
                        .append(" / ").append(c.sourceFile()).append("]: ").append(c.excerpt()).append("\n");
            }
            String userMessage = "Merged analysis: " + mapper.writeValueAsString(merged)
                    + "\n\nShortlist: " + mapper.writeValueAsString(shortlist)
                    + "\n\nRetrieved evidence:\n" + (citations.isEmpty() ? "(none found)" : evidenceBlock);

            RagValidationJudgement judgement = call(RAG_VALIDATION_SYSTEM_PROMPT, userMessage, RagValidationJudgement.class);
            return new RagValidationResult(citations, judgement.allClaimsSupported(), judgement.notes());
        } catch (Exception e) {
            throw new RuntimeException("RAG validation failed", e);
        }
    }

    private record RagValidationJudgement(boolean allClaimsSupported, String notes) {}

    // ── 8. Compliance Check Agent — "Is this recommendation compliant?" ────

    private static final String COMPLIANCE_SYSTEM_PROMPT = """
            You are the Compliance Check Agent in AIA Singapore's insurance
            recommendation engine — an automated compliance officer. Purpose:
            catch recommendations that would violate basic suitability rules
            before they reach the customer. Check exactly three things against the
            data provided:
              1. Affordability — does the shortlisted recommendation's likely
                 premium fit within the Affordability agent's estimated range?
              2. Eligibility — is there anything in the profile (age, occupation,
                 stated conditions) that would make the customer ineligible for
                 the shortlisted products, based on the product evidence provided?
              3. Need match — does the shortlist actually address the protection
                 gaps the Need agent identified?

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            {
              "compliant": true|false,
              "checks": [
                {"check": "Affordability", "passed": true|false, "note": "1 sentence"},
                {"check": "Eligibility", "passed": true|false, "note": "1 sentence"},
                {"check": "Need match", "passed": true|false, "note": "1 sentence"}
              ],
              "issues": ["short phrase per blocking issue found, empty array if none"],
              "rationale": "1-2 sentences on the overall verdict"
            }
            """;

    public ComplianceCheckResult checkCompliance(CustomerProfile profile, MergedInsights merged,
                                                  ProductShortlistResult shortlist, RagValidationResult validation) {
        try {
            String userMessage = "Customer profile:\n" + profileSummary(profile)
                    + "\n\nMerged analysis: " + mapper.writeValueAsString(merged)
                    + "\n\nShortlisted products: " + mapper.writeValueAsString(shortlist)
                    + "\n\nEvidence validation: " + mapper.writeValueAsString(validation);
            return call(COMPLIANCE_SYSTEM_PROMPT, userMessage, ComplianceCheckResult.class);
        } catch (Exception e) {
            throw new RuntimeException("Compliance check failed", e);
        }
    }

    // ── 9. Recommendation Summary Agent — "How do I explain this?" ─────────

    /**
     * Runs AFTER compliance, not before: the customer-facing pitch is written
     * with the compliance verdict already in hand, so a non-compliant run
     * produces a summary that's honestly caveated ("this needs financial
     * review before we proceed") instead of a confident sales pitch for a
     * recommendation compliance has already flagged.
     */
    private static final String SUMMARY_SYSTEM_PROMPT = """
            You are the Recommendation Summary Agent in AIA Singapore's insurance
            recommendation engine. Purpose: convert the technical pipeline results
            into an explanation the AIA agent can actually say to the customer —
            generate an understandable recommendation, not a data dump.

            You are given the Compliance Check Agent's verdict. If compliant is
            false, or any check failed, do NOT present the recommendation as a
            done deal: soften the language, and add a talking point that names
            the specific compliance issue(s) and states it needs to be resolved
            before proceeding. If compliant is true, write a confident, plain
            pitch as normal.

            You may also be given "Conversation signals" (sentiment and
            buying-signal trend measured live during the call). Use them only to
            tune tone and pacing (e.g. reassure a hesitant customer, move faster
            with an eager one) — never to change what is recommended.

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            {
              "customerFacingSummary": "2-4 sentences, plain language, no insurance jargon, as if speaking to the customer",
              "keyTalkingPoints": ["short, spoken-language bullet per key point the agent should raise"]
            }
            """;

    public RecommendationSummaryResult summarize(CustomerProfile profile, MergedInsights merged,
                                                  ProductShortlistResult shortlist, RagValidationResult validation,
                                                  ComplianceCheckResult compliance) {
        try {
            String userMessage = "Customer profile:\n" + profileSummary(profile)
                    + signalsBlock(profile)
                    + "\n\nMerged analysis: " + mapper.writeValueAsString(merged)
                    + "\n\nShortlisted products: " + mapper.writeValueAsString(shortlist)
                    + "\n\nEvidence validation: " + mapper.writeValueAsString(validation)
                    + "\n\nCompliance verdict: " + mapper.writeValueAsString(compliance);
            return call(SUMMARY_SYSTEM_PROMPT, userMessage, RecommendationSummaryResult.class);
        } catch (Exception e) {
            throw new RuntimeException("Summary generation failed", e);
        }
    }

    // ── 10. Sales Report Generation Agent — the final advisory report ──────

    /**
     * Deterministically assembles the full report from every prior step's
     * already-validated, already-stored output — see SalesReportResult's
     * Javadoc for why this isn't itself an LLM call: the report's job is to
     * present facts the pipeline already established correctly, not to
     * re-derive or restate them (and risk drifting from what was actually
     * found) through another generation pass.
     */
    public SalesReportResult generateSalesReport(CustomerProfile profile, MergedInsights merged,
                                                  CustomerPersonaResult persona, ProductScoringResult scoring,
                                                  ProductShortlistResult shortlist, RagValidationResult validation,
                                                  RecommendationSummaryResult summary, ComplianceCheckResult compliance) {
        String generatedAt = DateTimeFormatter.ofPattern("d MMM yyyy, h:mm a")
                .withZone(ZoneId.of("Asia/Singapore"))
                .format(java.time.Instant.now());

        StringBuilder md = new StringBuilder();
        md.append("# AIA Singapore — Advisory Sales Report\n\n");
        md.append("*Generated ").append(generatedAt).append(" SGT*\n\n");

        md.append("## Customer\n");
        md.append("- **Name:** ").append(nullToUnknown(profile.getCustomerName())).append("\n");
        md.append("- **Age:** ").append(profile.getAge() == null ? "Unknown" : profile.getAge()).append("\n");
        md.append("- **Occupation:** ").append(nullToUnknown(profile.getOccupation())).append("\n");
        md.append("- **Dependents:** ").append(profile.getDependents() == null ? "Unknown" : profile.getDependents()).append("\n");
        md.append("- **Persona:** ").append(persona.personaLabel()).append(" (").append(persona.lifeStage()).append(")\n\n");

        LiveInsightsSnapshot live = profile.getLiveInsights();
        if (live != null && live.latest() != null) {
            var sentiment = live.latest().sentiment();
            var buying = live.latest().buyingSignal();
            md.append("## Conversation Signals\n");
            md.append("- **Sentiment at end of call:** ").append(sentiment.label()).append(" (").append(sentiment.emotion()).append(")\n");
            md.append("- **Buying signal:** ").append(buying.level()).append(" — ").append(buying.score()).append("/100\n");
            for (String t : buying.signals()) md.append("  - ").append(t).append("\n");
            md.append("\n");
        }

        md.append("## Executive Summary\n").append(summary.customerFacingSummary()).append("\n\n");

        md.append("## Needs, Risk & Affordability\n");
        md.append("**Protection gaps:** ").append(String.join(", ", merged.needs().protectionGaps())).append("\n\n");
        md.append("**Risk level:** ").append(merged.risks().riskLevel())
                .append(" — ").append(String.join(", ", merged.risks().riskFactors())).append("\n\n");
        md.append("**Affordability:** ").append(merged.affordability().estimatedBudgetBand())
                .append(" (").append(merged.affordability().affordablePremiumRange()).append(")\n\n");

        md.append("## Product Scoring\n");
        for (ProductScore s : scoring.scores()) {
            md.append("- **").append(s.productName()).append("** — ").append(s.score()).append("/100\n");
        }
        md.append("\n");

        md.append("## Shortlisted Recommendations\n").append(shortlist.rationale()).append("\n\n");

        md.append("## Supporting Evidence\n");
        if (validation.citations().isEmpty()) {
            md.append("No supporting evidence was retrieved.\n\n");
        } else {
            for (EvidenceCitation c : validation.citations()) {
                md.append("- *").append(c.productName()).append(" — ").append(c.docCategory())
                        .append(" (").append(c.sourceFile()).append(")*: ").append(c.excerpt()).append("\n");
            }
            md.append("\n**Evidence-validated:** ").append(validation.allClaimsSupported() ? "Yes" : "Needs review")
                    .append(" — ").append(validation.notes()).append("\n\n");
        }

        md.append("## Talking Points for the Advisor\n");
        for (String point : summary.keyTalkingPoints()) md.append("- ").append(point).append("\n");
        md.append("\n");

        md.append("## Compliance Review\n");
        md.append("**Overall:** ").append(compliance.compliant() ? "COMPLIANT" : "REQUIRES REVIEW").append("\n\n");
        for (ComplianceCheckItem item : compliance.checks()) {
            md.append("- ").append(item.passed() ? "✓" : "✗").append(" **").append(item.check())
                    .append("** — ").append(item.note()).append("\n");
        }
        if (!compliance.issues().isEmpty()) {
            md.append("\n**Issues to resolve before proceeding:**\n");
            for (String issue : compliance.issues()) md.append("- ").append(issue).append("\n");
        }

        if (live != null && live.latest() != null && !live.latest().complianceFlags().isEmpty()) {
            md.append("\n## Advisor Conduct Flags (live monitoring — for review)\n");
            md.append("*Raised automatically during the conversation. Reported for advisor and compliance review; ")
                    .append("they did not change the recommendation above.*\n\n");
            for (var f : live.latest().complianceFlags()) {
                md.append("- **").append("high".equals(f.severity()) ? "High risk" : "Caution").append(":** “")
                        .append(f.statement()).append("” — ").append(f.advice()).append("\n");
            }
        }

        String title = "Advisory Sales Report — " + nullToUnknown(profile.getCustomerName());
        return new SalesReportResult(title, md.toString());
    }

    // ── Shared LLM call helper ───────────────────────────────────────────────

    private <T> T call(String systemPrompt, String userMessage, Class<T> type) {
        Exception lastError = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return callOnce(systemPrompt, userMessage, type);
            } catch (Exception e) {
                lastError = e;
                log.warn("LLM call attempt {}/{} failed for response type {}: {}",
                        attempt, MAX_ATTEMPTS, type.getSimpleName(), e.getMessage());
                if (attempt < MAX_ATTEMPTS) sleepBackoff(attempt);
            }
        }
        throw new RuntimeException("LLM call failed after " + MAX_ATTEMPTS + " attempts", lastError);
    }

    private <T> T callOnce(String systemPrompt, String userMessage, Class<T> type) throws Exception {
        var future = llmExecutor.submit(() -> chatModel.call(new Prompt(
                List.of(new SystemMessage(systemPrompt), new UserMessage(userMessage)),
                OpenAiChatOptions.builder().responseFormat(JSON_FORMAT).build())));

        ChatResponse response;
        try {
            response = future.get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            future.cancel(true);
            throw new RuntimeException("LLM call timed out after " + CALL_TIMEOUT, te);
        } catch (ExecutionException ee) {
            throw ee.getCause() instanceof Exception cause ? cause : ee;
        }

        String text = response.getResult().getOutput().getText();
        try {
            return mapper.readValue(text, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse agent JSON response: " + text, e);
        }
    }

    private void sleepBackoff(int attempt) {
        try {
            Thread.sleep(RETRY_BASE_DELAY.toMillis() * (1L << (attempt - 1)));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private String profileSearchQuery(CustomerProfile profile) {
        StringBuilder sb = new StringBuilder();
        if (profile.getOccupation() != null) sb.append(profile.getOccupation()).append(" ");
        if (profile.getGoalsAndConcerns() != null) sb.append(String.join(" ", profile.getGoalsAndConcerns())).append(" ");
        if (profile.getExistingPolicies() != null) sb.append(String.join(" ", profile.getExistingPolicies())).append(" ");
        if (profile.getNotes() != null) sb.append(profile.getNotes());
        String query = sb.toString().trim();
        return query.isBlank() ? "life insurance protection needs" : query;
    }

    // ── Live conversation signals (from the voice-session copilot) ─────────

    /**
     * Sentiment + buying-signal trend measured live during the call, as
     * supporting context for the Persona and Summary agents ONLY. Live needs,
     * product matches and fit scores are deliberately NOT passed on — the
     * agents must reach their own conclusions independently, not anchor on a
     * cheap first impression. Conduct flags are also excluded: they are
     * report-only (see the sales report) and never influence the analysis.
     */
    private String signalsBlock(CustomerProfile profile) {
        LiveInsightsSnapshot live = profile.getLiveInsights();
        if (live == null || live.latest() == null || live.history() == null || live.history().isEmpty()) return "";
        var h = live.history();
        int first = h.get(0).sentiment(), last = h.get(h.size() - 1).sentiment();
        int min = h.stream().mapToInt(LiveInsightsSnapshot.SignalPoint::sentiment).min().orElse(last);
        int max = h.stream().mapToInt(LiveInsightsSnapshot.SignalPoint::sentiment).max().orElse(last);
        var latest = live.latest();
        return "\n\nConversation signals (measured live, supporting context only):\n"
                + "- Sentiment trend (-100..100): started " + first + ", low " + min + ", high " + max + ", ended " + last
                + " (" + latest.sentiment().label() + ", " + latest.sentiment().emotion() + ")\n"
                + "- Buying signal at end of call: " + latest.buyingSignal().level() + " (" + latest.buyingSignal().score() + "/100)"
                + (latest.buyingSignal().signals().isEmpty() ? "" : "; triggers: " + String.join("; ", latest.buyingSignal().signals()))
                + "\n";
    }

    private String profileSummary(CustomerProfile profile) {
        return """
                Name: %s
                Age: %s
                Occupation: %s
                Income band: %s
                Dependents: %s
                Existing policies: %s
                Goals/concerns: %s
                Budget notes: %s
                Other notes: %s
                """.formatted(
                nullToUnknown(profile.getCustomerName()),
                profile.getAge() == null ? "Unknown" : profile.getAge().toString(),
                nullToUnknown(profile.getOccupation()),
                nullToUnknown(profile.getIncomeBand()),
                profile.getDependents() == null ? "Unknown" : profile.getDependents().toString(),
                listOrNone(profile.getExistingPolicies()),
                listOrNone(profile.getGoalsAndConcerns()),
                nullToUnknown(profile.getBudgetNotes()),
                nullToUnknown(profile.getNotes()));
    }

    private String productCandidatesBlock(List<ProductChunkMatch> candidates) {
        if (candidates.isEmpty()) return "(no matching product excerpts found)";
        StringBuilder sb = new StringBuilder();
        int i = 1;
        for (ProductChunkMatch c : candidates) {
            sb.append(i++).append(". [").append(c.productName()).append(" / ").append(c.docCategory())
                    .append(" / ").append(c.sectionTitle()).append("]\n")
                    .append(c.content()).append("\n\n");
        }
        return sb.toString();
    }

    private String excerpt(String content) {
        if (content == null) return "";
        String trimmed = content.strip();
        return trimmed.length() <= EVIDENCE_EXCERPT_MAX_CHARS ? trimmed : trimmed.substring(0, EVIDENCE_EXCERPT_MAX_CHARS) + "…";
    }

    private String nullToUnknown(String s) { return (s == null || s.isBlank()) ? "Unknown" : s; }
    private String listOrNone(List<String> list) { return (list == null || list.isEmpty()) ? "None mentioned" : String.join(", ", list); }
}
