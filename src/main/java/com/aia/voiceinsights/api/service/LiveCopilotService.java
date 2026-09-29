package com.aia.voiceinsights.api.service;

import com.aia.voiceinsights.api.model.CopilotInsights;
import com.aia.voiceinsights.api.model.CopilotInsights.*;
import com.aia.voiceinsights.api.model.CustomerProfile;
import com.aia.voiceinsights.api.model.ProductChunkMatch;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.ResponseFormat;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Real-time "advisor copilot": while the conversation is still going, reads
 * the transcript so far and produces need tags, customer sentiment, a buying
 * signal, next-best-question prompts and compliance flags in ONE LLM call
 * (cheap enough to run after every finalized segment), then grounds product
 * matches in the same pgvector catalog the post-call agents use — no extra
 * LLM call, so fit scores and evidence excerpts come straight from the
 * product documents. Best-effort: any failure returns null and the UI just
 * keeps showing the previous snapshot.
 */
@Service
public class LiveCopilotService {

    private static final Logger log = LoggerFactory.getLogger(LiveCopilotService.class);

    private static final ResponseFormat JSON_FORMAT =
            ResponseFormat.builder().type(ResponseFormat.Type.JSON_OBJECT).build();

    private static final int MAX_PRODUCTS = 3;
    private static final int EXCERPT_CHARS = 170;
    /** Only the tail of a long call is sent to the model — keeps latency flat as the call grows. */
    private static final int MAX_TRANSCRIPT_CHARS = 6000;

    private static final String SYSTEM_PROMPT = ("""
            You are a real-time copilot for an AIA insurance advisor, reading a live,
            speaker-unlabelled transcript of the advisor talking with a customer.
            Infer who is speaking from context. Be concise and never invent facts.

            Respond with ONLY a JSON object, no markdown fences:
            {
              "needs": [ {"label": "EXACTLY one of: %s", "strength": 0-100} ],
              "sentiment": {"score": -100..100, "label": "Negative|Concerned|Neutral|Positive|Very positive", "emotion": "one word, e.g. Anxious, Curious, Hopeful, Skeptical, Confident"},
              "buyingSignal": {"score": 0-100, "level": "Cold|Warm|Hot", "signals": ["short paraphrase of what the customer said/did that indicates intent, max 3"]},
              "nextQuestions": ["up to 3 short questions the advisor should ask next, based on what is still unknown (existing coverage, budget, dependents, health, timeline, goals)"],
              "complianceFlags": [ {"severity": "high|medium", "statement": "the risky thing the ADVISOR said, quoted briefly", "advice": "one short corrective suggestion"} ]
            }
            Rules:
            - needs: at most 5, strongest first, only needs actually evidenced.
            - buyingSignal: Hot = asks price/how to start/compares plans/agrees to next step; Warm = engaged and asking product questions; Cold = passive or objecting.
            - sentiment: the customer's CURRENT mood, weighted toward their last 2-3 turns — move it promptly when their tone genuinely changes (e.g. relief, enthusiasm, objection), but do not swing on a single neutral advisor line.
            - Stability: you are given the PREVIOUS analysis. Keep scores, needs and questions the same unless the new transcript gives real evidence to change them — never re-rate from scratch.
            - nextQuestions: you are given facts ALREADY KNOWN, the questions currently suggested, and RETIRED questions. Keep a current question ONLY if it is still unanswered; replace answered ones with a NEW topic; never suggest anything about a known fact, and never repeat or rephrase a retired question. Fewer than 3 (even 0) is fine if nothing useful is left to ask.
            - complianceFlags: ONLY for statements by the advisor such as guaranteed returns, promises of approval or claim payout, misleading comparisons, pressure tactics, or advice beyond suitability. Empty array if none. Never flag the customer.
            """).formatted(NeedTaxonomy.asPromptList());

    private record Llm(List<NeedTag> needs, Sentiment sentiment, BuyingSignal buyingSignal,
                       List<String> nextQuestions, List<ComplianceFlag> complianceFlags) {}

    /** Per-call memory: last smoothed snapshot plus the question history used to stop repeats. */
    public static class State {
        private CopilotInsights previous;
        private final Set<String> retiredQuestions = new LinkedHashSet<>();
        private List<String> currentQuestions = List.of();
        private final Map<String, Double> fitByProduct = new LinkedHashMap<>();
    }

    // Exponential smoothing weights (share given to the NEW reading). Sentiment reacts fastest,
    // since a real change of mood should show up within a turn or two.
    private static final double ALPHA_SENTIMENT = 0.6;
    private static final double ALPHA_BUYING = 0.5;
    private static final double ALPHA_NEED = 0.5;
    private static final double ALPHA_FIT = 0.4;
    private static final int LEVEL_HYSTERESIS = 6;

    private final ChatModel chatModel;
    private final ProductVectorSearchService productSearch;
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public LiveCopilotService(ChatModel chatModel, ProductVectorSearchService productSearch) {
        this.chatModel = chatModel;
        this.productSearch = productSearch;
    }

    public CopilotInsights analyse(CustomerProfile profile, String transcript, State state) {
        if (transcript == null || transcript.isBlank()) return null;
        try {
            String tail = transcript.length() > MAX_TRANSCRIPT_CHARS
                    ? transcript.substring(transcript.length() - MAX_TRANSCRIPT_CHARS) : transcript;
            var response = chatModel.call(new Prompt(
                    List.of(new SystemMessage(SYSTEM_PROMPT), new UserMessage(buildUserMessage(profile, tail, state))),
                    OpenAiChatOptions.builder().responseFormat(JSON_FORMAT).build()));
            Llm llm = mapper.readValue(response.getResult().getOutput().getText(), Llm.class);

            CopilotInsights prev = state.previous;
            List<NeedTag> needs = smoothNeeds(prev == null ? List.of() : prev.needs(),
                    llm.needs() == null ? List.of() : llm.needs());
            Sentiment sentiment = smoothSentiment(prev == null ? null : prev.sentiment(), llm.sentiment());
            BuyingSignal buying = smoothBuying(prev == null ? null : prev.buyingSignal(), llm.buyingSignal());
            List<String> questions = pickQuestions(state, llm.nextQuestions());
            List<ComplianceFlag> flags = mergeFlags(prev == null ? List.of() : prev.complianceFlags(),
                    llm.complianceFlags() == null ? List.of() : llm.complianceFlags());

            CopilotInsights out = new CopilotInsights(needs, sentiment, buying, questions, flags,
                    matchProducts(profile, needs, tail, state));
            state.previous = out;
            return out;
        } catch (Exception e) {
            log.warn("LiveCopilotService: analysis failed: {}", e.toString());
            return null;
        }
    }

    private String buildUserMessage(CustomerProfile p, String tail, State state) {
        StringBuilder sb = new StringBuilder();
        sb.append("FACTS ALREADY KNOWN (from the profile so far): ").append(knownFacts(p)).append("\n");
        sb.append("QUESTIONS CURRENTLY SUGGESTED: ").append(state.currentQuestions).append("\n");
        sb.append("RETIRED QUESTIONS (answered or dropped — never suggest again): ").append(state.retiredQuestions).append("\n");
        if (state.previous != null) {
            try {
                sb.append("PREVIOUS ANALYSIS: ").append(mapper.writeValueAsString(new Llm(state.previous.needs(),
                        state.previous.sentiment(), state.previous.buyingSignal(), state.previous.nextQuestions(),
                        state.previous.complianceFlags()))).append("\n");
            } catch (Exception ignored) {
                // Prompt context only — analysis still works without it.
            }
        }
        return sb.append("\nTranscript so far:\n").append(tail).toString();
    }

    private String knownFacts(CustomerProfile p) {
        Map<String, Object> facts = new LinkedHashMap<>();
        if (p.getCustomerName() != null) facts.put("name", p.getCustomerName());
        if (p.getAge() != null) facts.put("age", p.getAge());
        if (p.getOccupation() != null) facts.put("occupation", p.getOccupation());
        if (p.getIncomeBand() != null) facts.put("incomeBand", p.getIncomeBand());
        if (p.getDependents() != null) facts.put("dependents", p.getDependents());
        if (p.getExistingPolicies() != null && !p.getExistingPolicies().isEmpty()) facts.put("existingPolicies", p.getExistingPolicies());
        if (p.getGoalsAndConcerns() != null && !p.getGoalsAndConcerns().isEmpty()) facts.put("goalsAndConcerns", p.getGoalsAndConcerns());
        if (p.getBudgetNotes() != null) facts.put("budget", p.getBudgetNotes());
        return facts.isEmpty() ? "none yet" : facts.toString();
    }

    // ── Smoothing ────────────────────────────────────────────────────────

    private static int clamp(double v, int lo, int hi) {
        return (int) Math.max(lo, Math.min(hi, Math.round(v)));
    }

    private static double ema(double prev, double next, double alpha) {
        return prev + alpha * (next - prev);
    }

    private Sentiment smoothSentiment(Sentiment prev, Sentiment next) {
        if (next == null) return prev != null ? prev : new Sentiment(0, "Neutral", "Neutral");
        int score = prev == null ? clamp(next.score(), -100, 100) : clamp(ema(prev.score(), next.score(), ALPHA_SENTIMENT), -100, 100);
        // The label follows the smoothed score so the word and the gauge can never disagree.
        String label = score <= -55 ? "Negative" : score <= -5 ? "Concerned" : score < 12 ? "Neutral" : score < 50 ? "Positive" : "Very positive";
        return new Sentiment(score, label, next.emotion() == null ? "Neutral" : next.emotion());
    }

    private BuyingSignal smoothBuying(BuyingSignal prev, BuyingSignal next) {
        if (next == null) return prev != null ? prev : new BuyingSignal(0, "Cold", List.of());
        int score = prev == null ? clamp(next.score(), 0, 100) : clamp(ema(prev.score(), next.score(), ALPHA_BUYING), 0, 100);
        String level = levelFor(score);
        // Hysteresis: hovering near a boundary must not flap Warm <-> Hot.
        if (prev != null && !level.equals(prev.level())) {
            int boundary = level.equals("Hot") || prev.level().equals("Hot") ? 70 : 35;
            if (Math.abs(score - boundary) < LEVEL_HYSTERESIS) level = prev.level();
        }
        return new BuyingSignal(score, level, next.signals() == null ? List.of() : next.signals());
    }

    private String levelFor(int score) {
        return score >= 70 ? "Hot" : score >= 35 ? "Warm" : "Cold";
    }

    /** Needs smooth by label; a need not re-mentioned decays instead of vanishing instantly. */
    private List<NeedTag> smoothNeeds(List<NeedTag> prev, List<NeedTag> next) {
        Map<String, Double> merged = new LinkedHashMap<>();
        Map<String, Double> prevMap = new LinkedHashMap<>();
        prev.forEach(n -> prevMap.put(n.label(), (double) n.strength()));
        Set<String> seen = new HashSet<>();
        for (NeedTag n : next) {
            seen.add(n.label());
            double p = prevMap.getOrDefault(n.label(), (double) n.strength());
            merged.put(n.label(), ema(p, n.strength(), ALPHA_NEED));
        }
        prevMap.forEach((label, v) -> { if (!seen.contains(label)) merged.put(label, v * 0.85); });
        return merged.entrySet().stream()
                .filter(e -> e.getValue() >= 25)
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .limit(5)
                .map(e -> new NeedTag(e.getKey(), clamp(e.getValue(), 0, 100)))
                .toList();
    }

    /** A compliance flag is a fact about what was said — once raised it stays for the call. */
    private List<ComplianceFlag> mergeFlags(List<ComplianceFlag> prev, List<ComplianceFlag> next) {
        List<ComplianceFlag> out = new ArrayList<>(prev);
        for (ComplianceFlag f : next) {
            boolean dup = out.stream().anyMatch(o -> similar(o.statement(), f.statement()));
            if (!dup) out.add(f);
        }
        return out;
    }

    // ── Questions ────────────────────────────────────────────────────────

    private List<String> pickQuestions(State state, List<String> proposed) {
        List<String> picked = new ArrayList<>();
        if (proposed != null) {
            for (String q : proposed) {
                if (q == null || q.isBlank() || picked.size() == 3) continue;
                boolean retired = state.retiredQuestions.stream().anyMatch(r -> similar(r, q));
                boolean dup = picked.stream().anyMatch(x -> similar(x, q));
                if (!retired && !dup) picked.add(q.trim());
            }
        }
        // Keep the previous stable set if nothing valid came back — but never re-show a retired one.
        if (picked.isEmpty() && proposed == null) return state.currentQuestions;

        // Anything shown before that is no longer suggested is now answered/dropped: retire it.
        for (String old : state.currentQuestions) {
            if (picked.stream().noneMatch(q -> similar(q, old))) state.retiredQuestions.add(old);
        }
        state.currentQuestions = List.copyOf(picked);
        return state.currentQuestions;
    }

    /** Word-overlap similarity — catches rephrasings like "any existing coverage?" vs "existing insurance coverage?". */
    private boolean similar(String a, String b) {
        if (a == null || b == null) return false;
        Set<String> wa = words(a), wb = words(b);
        if (wa.isEmpty() || wb.isEmpty()) return false;
        Set<String> inter = new HashSet<>(wa);
        inter.retainAll(wb);
        double jaccard = (double) inter.size() / (wa.size() + wb.size() - inter.size());
        return jaccard >= 0.5;
    }

    private static final Set<String> STOP = Set.of("a", "an", "the", "do", "does", "you", "your", "are", "is", "what", "how",
            "any", "of", "to", "for", "in", "on", "and", "or", "have", "has", "it", "that", "this", "with", "about", "there", "would", "like", "can", "we", "i");

    private Set<String> words(String s) {
        Set<String> out = new HashSet<>();
        for (String w : s.toLowerCase().replaceAll("[^a-z0-9 ]", " ").split("\\s+")) {
            if (w.length() > 2 && !STOP.contains(w)) out.add(w);
        }
        return out;
    }

    private List<ProductMatch> matchProducts(CustomerProfile profile, List<NeedTag> needs, String transcriptTail, State state) {
        try {
            StringBuilder q = new StringBuilder();
            needs.forEach(n -> q.append(n.label()).append(". "));
            if (profile.getGoalsAndConcerns() != null) profile.getGoalsAndConcerns().forEach(g -> q.append(g).append(". "));
            if (q.isEmpty()) {
                q.append(transcriptTail.substring(Math.max(0, transcriptTail.length() - 400)));
            }

            // Best chunk per product — search() is already ordered by similarity.
            Map<String, ProductChunkMatch> best = new LinkedHashMap<>();
            for (ProductChunkMatch m : productSearch.search(q.toString())) best.putIfAbsent(m.productName(), m);

            List<ProductMatch> out = new ArrayList<>();
            for (ProductChunkMatch m : best.values()) {
                if (out.size() == MAX_PRODUCTS) break;
                // Raw cosine similarity for short need-phrases against long document chunks sits
                // roughly in 0.3-0.6 — stretch that band into a readable 0-100 fit.
                double raw = Math.max(1, Math.min(99, (m.similarity() - 0.15) / 0.45 * 100));
                // Smoothed per product so the ring doesn't jump ±10 as the query wording shifts turn to turn.
                double smoothed = ema(state.fitByProduct.getOrDefault(m.productName(), raw), raw, ALPHA_FIT);
                state.fitByProduct.put(m.productName(), smoothed);
                out.add(new ProductMatch(m.productName(), clamp(smoothed, 1, 99), excerpt(m.content()), m.sourceFile()));
            }
            out.sort((x, y) -> Integer.compare(y.fitScore(), x.fitScore()));
            return out;
        } catch (Exception e) {
            log.warn("LiveCopilotService: product matching failed: {}", e.toString());
            return List.of();
        }
    }

    private String excerpt(String content) {
        if (content == null) return "";
        String flat = content.replaceAll("\\s+", " ").trim();
        return flat.length() <= EXCERPT_CHARS ? flat : flat.substring(0, EXCERPT_CHARS).trim() + "…";
    }
}
