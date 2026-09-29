package com.aia.voiceinsights.api.config;

import com.aia.voiceinsights.api.service.AuthStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;

/**
 * Enforces AuthStore's session token on every {@code /api/**} request except
 * {@code /api/auth/**} (signup/login must be reachable unauthenticated). The
 * token is read from the {@code X-Session-Token} header, or from a {@code
 * token} query parameter as a fallback — the browser's EventSource and
 * anchor-download APIs (used for the live recommendation stream and the
 * sales report download) can't attach custom headers, so those two GET
 * endpoints are only reachable via the query-param form in practice.
 *
 * Deliberately does NOT cover {@code /ws/**} (voice capture) yet: the
 * customer profile doesn't exist before that call, so it carries no PII by
 * itself, and browser WebSocket also can't set custom headers — securing it
 * needs the same query-param treatment plus a frontend change, tracked as a
 * follow-up rather than bundled into this pass.
 */
@Component
public class SessionAuthFilter extends OncePerRequestFilter {

    public static final String USER_ID_ATTR = "vi.authUserId";
    private static final String TOKEN_HEADER = "X-Session-Token";
    private static final String TOKEN_PARAM = "token";

    private static final Logger log = LoggerFactory.getLogger(SessionAuthFilter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final AuthStore authStore;

    public SessionAuthFilter(AuthStore authStore) {
        this.authStore = authStore;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // CORS preflight carries no auth header by design (and no side effects) —
        // blocking it here would fail the browser's actual request before it's
        // even sent, for every cross-origin call that sets X-Session-Token.
        if ("OPTIONS".equalsIgnoreCase(request.getMethod())) return true;

        String path = request.getRequestURI();
        return !path.startsWith("/api/") || path.startsWith("/api/auth/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String token = request.getHeader(TOKEN_HEADER);
        if (token == null || token.isBlank()) token = request.getParameter(TOKEN_PARAM);

        var userId = authStore.resolveUserId(token);
        if (userId.isEmpty()) {
            log.warn("Rejected unauthenticated request to {}", request.getRequestURI());
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.getWriter().write(mapper.writeValueAsString(Map.of("error", "Not authenticated")));
            return;
        }

        request.setAttribute(USER_ID_ATTR, userId.get());
        chain.doFilter(request, response);
    }
}
