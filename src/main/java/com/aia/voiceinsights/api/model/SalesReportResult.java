package com.aia.voiceinsights.api.model;

import java.io.Serializable;

/**
 * Sales Report Generation Agent — the final advisory sales report, assembled
 * deterministically from every prior agent's stored output (customer
 * profile, needs/risks/affordability, persona, product scoring/shortlist,
 * RAG evidence, compliance verdict) with only the narrative prose
 * (executive summary, advisor talking points) LLM-generated — see
 * RecommendationAgentService.generateSalesReport for why: letting an LLM
 * restate figures/facts that are already known and stored risks drift
 * between what the report says and what the pipeline actually found.
 */
public record SalesReportResult(
    String title,
    String reportMarkdown
) implements Serializable {}
