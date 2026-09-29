package com.aia.voiceinsights.api.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

/**
 * Fixed-window request counter backed by the Redis instance this service
 * already runs for session caching — INCR the current window's key, and let
 * the first writer in a window set its expiry. Simple fixed-window counting
 * (not sliding/token-bucket) is a deliberate choice: the only thing this
 * guards today is "don't let one advisor account fire an unbounded number of
 * ~9-LLM-call recommendation runs," where a window-edge burst is an
 * acceptable trade for not adding a second Redis data structure.
 */
@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);
    private static final String KEY_PREFIX = "vi:ratelimit:";

    private final StringRedisTemplate redisTemplate;

    public RateLimiterService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /** Returns true if this call is within the {@code limit} for the current {@code window}, false if it should be rejected. */
    public boolean tryConsume(String key, int limit, Duration window) {
        long windowIndex = Instant.now().getEpochSecond() / window.getSeconds();
        String redisKey = KEY_PREFIX + key + ":" + windowIndex;
        try {
            Long count = redisTemplate.opsForValue().increment(redisKey);
            if (count != null && count == 1L) {
                redisTemplate.expire(redisKey, window);
            }
            boolean allowed = count == null || count <= limit;
            if (!allowed) log.warn("Rate limit exceeded for key {} ({} > {})", key, count, limit);
            return allowed;
        } catch (Exception e) {
            // Redis unavailable — fail open rather than blocking the whole
            // recommendation flow on a cache outage.
            log.warn("Rate limiter check failed for key {}, allowing request: {}", key, e.getMessage());
            return true;
        }
    }
}
