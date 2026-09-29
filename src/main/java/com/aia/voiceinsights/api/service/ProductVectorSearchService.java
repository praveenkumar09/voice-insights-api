package com.aia.voiceinsights.api.service;

import com.aia.voiceinsights.api.model.ProductChunkMatch;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Cosine-similarity search over voice_insights.product_chunks — populated by
 * voice-insights-ingestor. Mirrors cobalt-rag-api's VectorSearchService's
 * pgvector query shape, minus hybrid keyword search + RRF + reranking (the
 * product catalog here is small enough that plain top-K vector search is
 * sufficient for the Need agent's retrieval tool).
 */
@Service
public class ProductVectorSearchService {

    private final JdbcTemplate jdbc;
    private final EmbeddingModel embeddingModel;
    private final String schema;

    @Value("${voice.product-search.top-k:8}")
    private int topK;

    @Value("${voice.product-search.similarity-threshold:0.3}")
    private double similarityThreshold;

    private static final String VECTOR_SQL_TEMPLATE = """
            SELECT chunk_id, product_name, doc_category, source_file, section_title, content,
                   1 - (embedding <=> ?::vector) AS similarity
            FROM %s.product_chunks
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> ?::vector
            LIMIT ?
            """;

    private static final String VECTOR_SQL_WITHIN_PRODUCT_TEMPLATE = """
            SELECT chunk_id, product_name, doc_category, source_file, section_title, content,
                   1 - (embedding <=> ?::vector) AS similarity
            FROM %s.product_chunks
            WHERE embedding IS NOT NULL AND product_name = ?
            ORDER BY embedding <=> ?::vector
            LIMIT ?
            """;

    public ProductVectorSearchService(JdbcTemplate jdbc, EmbeddingModel embeddingModel,
                                       @Value("${voice.db.schema:voice_insights}") String schema) {
        this.jdbc = jdbc;
        this.embeddingModel = embeddingModel;
        this.schema = schema;
    }

    /** Embeds {@code query} and returns the top matching product chunks above the similarity threshold. */
    public List<ProductChunkMatch> search(String query) {
        float[] queryEmbedding = embeddingModel.embed(query);
        String vectorLiteral = toVectorLiteral(queryEmbedding);

        List<ProductChunkMatch> matches = jdbc.query(
                VECTOR_SQL_TEMPLATE.formatted(schema), this::mapRow, vectorLiteral, vectorLiteral, topK);

        return matches.stream().filter(m -> m.similarity() >= similarityThreshold).toList();
    }

    /**
     * Same as {@link #search}, but restricted to one product's own chunks —
     * used by the RAG Validation agent so evidence cited for a recommended
     * product can never accidentally be pulled from a *different* product's
     * documents.
     */
    public List<ProductChunkMatch> searchWithinProduct(String productName, String query, int limit) {
        float[] queryEmbedding = embeddingModel.embed(query);
        String vectorLiteral = toVectorLiteral(queryEmbedding);

        List<ProductChunkMatch> matches = jdbc.query(
                VECTOR_SQL_WITHIN_PRODUCT_TEMPLATE.formatted(schema), this::mapRow,
                vectorLiteral, productName, vectorLiteral, limit);

        return matches.stream().filter(m -> m.similarity() >= similarityThreshold).toList();
    }

    /** Every distinct product in the catalog — the Product Scoring agent scores each one. */
    public List<String> listProductNames() {
        return jdbc.queryForList(
                "SELECT DISTINCT product_name FROM %s.product_chunks ORDER BY product_name".formatted(schema),
                String.class);
    }

    /**
     * A compact per-product profile (Product Summary / Fact Sheet chunks
     * preferred) — enough grounded context to score or shortlist a product
     * against a customer without pulling its entire document set into the
     * prompt.
     */
    public List<ProductChunkMatch> getProductOverview(String productName) {
        return jdbc.query("""
                SELECT chunk_id, product_name, doc_category, source_file, section_title, content, 1.0 AS similarity
                FROM %s.product_chunks
                WHERE product_name = ?
                ORDER BY CASE doc_category
                             WHEN 'Product Summary' THEN 0
                             WHEN 'Fact Sheet' THEN 1
                             WHEN 'Rider' THEN 2
                             ELSE 3
                         END, chunk_index
                LIMIT 6
                """.formatted(schema), this::mapRow, productName);
    }

    private ProductChunkMatch mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new ProductChunkMatch(
                rs.getString("chunk_id"),
                rs.getString("product_name"),
                rs.getString("doc_category"),
                rs.getString("source_file"),
                rs.getString("section_title"),
                rs.getString("content"),
                rs.getDouble("similarity"));
    }

    private String toVectorLiteral(float[] vec) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vec.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(vec[i]);
        }
        return sb.append(']').toString();
    }
}
