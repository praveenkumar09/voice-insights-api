package com.aia.voiceinsights.api.service;

import com.aia.voiceinsights.api.model.CustomerProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Self-provisioned (see {@link #init}) store for customer profiles captured
 * during a voice session, in this service's own {@code voice_insights} schema.
 * The full profile is kept as JSONB — {@code agent_user_id}/{@code status}
 * are broken out as real columns purely so they can be filtered/indexed.
 */
@Service
public class CustomerProfileStore {

    private final JdbcTemplate jdbc;
    private final String schema;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    public CustomerProfileStore(JdbcTemplate jdbc, @Value("${voice.db.schema:voice_insights}") String schema) {
        this.jdbc = jdbc;
        this.schema = schema;
    }

    @PostConstruct
    public void init() {
        jdbc.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS %s.customer_profiles (
                    id             TEXT PRIMARY KEY,
                    agent_user_id  TEXT,
                    status         TEXT NOT NULL DEFAULT 'IN_PROGRESS',
                    profile_json   JSONB NOT NULL,
                    raw_transcript TEXT,
                    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """.formatted(schema));
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_vi_profiles_agent ON %s.customer_profiles (agent_user_id)".formatted(schema));
    }

    /** Assigns an id if missing, sets created/updated timestamps, and upserts. */
    public CustomerProfile save(CustomerProfile profile) {
        Instant now = Instant.now();
        if (profile.getId() == null || profile.getId().isBlank()) {
            profile.setId(UUID.randomUUID().toString());
        }
        if (profile.getCreatedAt() == null) profile.setCreatedAt(now);
        profile.setUpdatedAt(now);

        try {
            String json = mapper.writeValueAsString(profile);
            jdbc.update("""
                    INSERT INTO %s.customer_profiles (id, agent_user_id, status, profile_json, raw_transcript, created_at, updated_at)
                    VALUES (?, ?, ?, ?::jsonb, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        agent_user_id  = EXCLUDED.agent_user_id,
                        status         = EXCLUDED.status,
                        profile_json   = EXCLUDED.profile_json,
                        raw_transcript = EXCLUDED.raw_transcript,
                        updated_at     = EXCLUDED.updated_at
                    """.formatted(schema),
                    profile.getId(), profile.getAgentUserId(), profile.getStatus(), json,
                    profile.getRawTranscript(), java.sql.Timestamp.from(profile.getCreatedAt()), java.sql.Timestamp.from(profile.getUpdatedAt()));
            return profile;
        } catch (Exception e) {
            throw new RuntimeException("Failed to save customer profile", e);
        }
    }

    public Optional<CustomerProfile> findById(String id) {
        List<String> rows = jdbc.query(
                "SELECT profile_json::text FROM %s.customer_profiles WHERE id = ?".formatted(schema),
                (rs, rowNum) -> rs.getString(1), id);
        if (rows.isEmpty()) return Optional.empty();
        return Optional.of(deserialize(rows.get(0)));
    }

    /** Most-recently-updated first — newest voice sessions surface at the top of the admin list. */
    public List<CustomerProfile> findAll(int page, int size) {
        List<String> rows = jdbc.query(
                "SELECT profile_json::text FROM %s.customer_profiles ORDER BY updated_at DESC LIMIT ? OFFSET ?"
                        .formatted(schema),
                (rs, rowNum) -> rs.getString(1), size, page * size);
        return rows.stream().map(this::deserialize).toList();
    }

    public long count() {
        Long total = jdbc.queryForObject("SELECT COUNT(*) FROM %s.customer_profiles".formatted(schema), Long.class);
        return total == null ? 0 : total;
    }

    private CustomerProfile deserialize(String json) {
        try {
            return mapper.readValue(json, CustomerProfile.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize customer profile", e);
        }
    }
}
