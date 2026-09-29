package com.aia.voiceinsights.api.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Same auth mechanism as cobalt-rag-api's AuthStore (BCrypt, email+password,
 * Postgres-backed session token with Redis cache-aside resolution) — but
 * self-provisioned under this service's own {@code voice_insights} schema,
 * so no user accounts or session rows are shared between the two apps even
 * though both ultimately live on the same Postgres server.
 */
@Service
public class AuthStore {

    private static final int SESSION_TTL_DAYS = 30;
    private static final String SESSION_CACHE_PREFIX = "vi:session:";

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redisTemplate;
    private final Duration cacheTtl;
    private final String schema;
    private final BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    public AuthStore(JdbcTemplate jdbc, StringRedisTemplate redisTemplate,
                      @Value("${voice.redis.session-cache-ttl-seconds:300}") long cacheTtlSeconds,
                      @Value("${voice.db.schema:voice_insights}") String schema) {
        this.jdbc = jdbc;
        this.redisTemplate = redisTemplate;
        this.cacheTtl = Duration.ofSeconds(cacheTtlSeconds);
        this.schema = schema;
    }

    public static class EmailAlreadyExistsException extends RuntimeException {}
    public static class InvalidCredentialsException extends RuntimeException {}
    public static class UserNotFoundException extends RuntimeException {}

    @PostConstruct
    public void init() {
        jdbc.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS %s.users (
                    id            TEXT PRIMARY KEY,
                    email         TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """.formatted(schema));
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS %s.sessions (
                    token      TEXT PRIMARY KEY,
                    user_id    TEXT NOT NULL REFERENCES %s.users(id) ON DELETE CASCADE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at TIMESTAMPTZ NOT NULL
                )
                """.formatted(schema, schema));
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_vi_sessions_user ON %s.sessions (user_id)".formatted(schema));

        // Same boot-wipe policy as cobalt-rag-api's AuthStore — every fresh
        // boot forces a fresh sign-in rather than resuming pre-restart sessions.
        jdbc.update("DELETE FROM %s.sessions".formatted(schema));
        try {
            Set<String> staleKeys = redisTemplate.keys(SESSION_CACHE_PREFIX + "*");
            if (staleKeys != null && !staleKeys.isEmpty()) redisTemplate.delete(staleKeys);
        } catch (Exception e) {
            // Redis unavailable at boot — the Postgres wipe above is still correct.
        }
    }

    public String signup(String email, String password) {
        String normalized = normalizeEmail(email);
        Integer exists = jdbc.queryForObject(
                "SELECT COUNT(*) FROM %s.users WHERE email = ?".formatted(schema), Integer.class, normalized);
        if (exists != null && exists > 0) throw new EmailAlreadyExistsException();

        String userId = UUID.randomUUID().toString();
        jdbc.update("INSERT INTO %s.users (id, email, password_hash) VALUES (?, ?, ?)".formatted(schema),
                userId, normalized, passwordEncoder.encode(password));
        return createSession(userId);
    }

    public String login(String email, String password) {
        String normalized = normalizeEmail(email);
        List<UserRow> rows = jdbc.query(
                "SELECT id, password_hash FROM %s.users WHERE email = ?".formatted(schema),
                (rs, rowNum) -> new UserRow(rs.getString("id"), rs.getString("password_hash")),
                normalized);
        if (rows.isEmpty() || !passwordEncoder.matches(password, rows.get(0).passwordHash())) {
            throw new InvalidCredentialsException();
        }
        return createSession(rows.get(0).id());
    }

    public void resetPassword(String email, String newPassword) {
        String normalized = normalizeEmail(email);
        List<String> ids = jdbc.query(
                "SELECT id FROM %s.users WHERE email = ?".formatted(schema), (rs, rowNum) -> rs.getString("id"), normalized);
        if (ids.isEmpty()) throw new UserNotFoundException();

        String userId = ids.get(0);
        List<String> tokens = jdbc.query(
                "SELECT token FROM %s.sessions WHERE user_id = ?".formatted(schema), (rs, rowNum) -> rs.getString("token"), userId);
        jdbc.update("UPDATE %s.users SET password_hash = ? WHERE id = ?".formatted(schema), passwordEncoder.encode(newPassword), userId);
        jdbc.update("DELETE FROM %s.sessions WHERE user_id = ?".formatted(schema), userId);
        evictCachedSessions(tokens);
    }

    public Optional<String> resolveUserId(String token) {
        if (token == null || token.isBlank()) return Optional.empty();
        try {
            String cached = redisTemplate.opsForValue().get(SESSION_CACHE_PREFIX + token);
            if (cached != null) return Optional.of(cached);
        } catch (Exception e) {
            // Redis down — fall through to Postgres.
        }

        List<String> ids = jdbc.query(
                "SELECT user_id FROM %s.sessions WHERE token = ? AND expires_at > now()".formatted(schema),
                (rs, rowNum) -> rs.getString("user_id"), token);
        if (ids.isEmpty()) return Optional.empty();
        String userId = ids.get(0);
        cacheSession(token, userId);
        return Optional.of(userId);
    }

    public String requireUserId(String token) {
        return resolveUserId(token).orElseThrow(InvalidCredentialsException::new);
    }

    public Optional<String> emailForUser(String userId) {
        List<String> emails = jdbc.query(
                "SELECT email FROM %s.users WHERE id = ?".formatted(schema), (rs, rowNum) -> rs.getString("email"), userId);
        return emails.isEmpty() ? Optional.empty() : Optional.of(emails.get(0));
    }

    public void logout(String token) {
        jdbc.update("DELETE FROM %s.sessions WHERE token = ?".formatted(schema), token);
        evictCachedSessions(List.of(token));
    }

    private String createSession(String userId) {
        String token = UUID.randomUUID().toString();
        jdbc.update(
                "INSERT INTO %s.sessions (token, user_id, expires_at) VALUES (?, ?, now() + make_interval(days => ?))".formatted(schema),
                token, userId, SESSION_TTL_DAYS);
        cacheSession(token, userId);
        return token;
    }

    private void cacheSession(String token, String userId) {
        try {
            redisTemplate.opsForValue().set(SESSION_CACHE_PREFIX + token, userId, cacheTtl);
        } catch (Exception e) {
            // Best-effort.
        }
    }

    private void evictCachedSessions(List<String> tokens) {
        try {
            redisTemplate.delete(tokens.stream().map(t -> SESSION_CACHE_PREFIX + t).toList());
        } catch (Exception e) {
            // Best-effort.
        }
    }

    private String normalizeEmail(String email) {
        return email == null ? "" : email.trim().toLowerCase();
    }

    private record UserRow(String id, String passwordHash) {}
}
