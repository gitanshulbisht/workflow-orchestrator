package com.buildathon.orchestrator.ratelimit;

import com.buildathon.orchestrator.config.OrchestratorProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Redis-backed rate limiting. Keys on the X-API-Key header (falling back to
 * the client IP). Rejections carry 429 + Retry-After.
 */
@Component
public class RateLimitFilter extends OncePerRequestFilter {

    public static final String API_KEY_HEADER = "X-API-Key";

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    private final RedissonClient redisson;
    private final OrchestratorProperties properties;

    public RateLimitFilter(RedissonClient redisson, OrchestratorProperties properties) {
        this.redisson = redisson;
        this.properties = properties;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        if (!properties.rateLimit().enabled()) {
            return true;
        }
        String path = request.getRequestURI();
        return !path.startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String key = request.getHeader(API_KEY_HEADER);
        if (key == null || key.isBlank()) {
            key = "ip:" + request.getRemoteAddr();
        } else {
            key = "key:" + key;
        }
        RRateLimiter limiter = redisson.getRateLimiter("orchestrator:ratelimit:" + key);
        boolean initialized = limiter.trySetRate(RateType.OVERALL,
                properties.rateLimit().permitsPerSecond(), 1, RateIntervalUnit.SECONDS);
        if (initialized) {
            limiter.expire(java.time.Duration.ofMinutes(5));
        }
        if (limiter.tryAcquire()) {
            chain.doFilter(request, response);
        } else {
            long retryAfter = Math.max(1, 1_000 / Math.max(1, properties.rateLimit().permitsPerSecond()));
            response.setStatus(429);
            response.setHeader("Retry-After", String.valueOf(retryAfter));
            response.setContentType("application/json");
            response.getWriter().write("""
                    {"type":"about:blank","title":"Too many requests","status":429,"detail":"Rate limit exceeded. Retry after the indicated delay."}
                    """);
            log.debug("Rate limit hit for {}", key);
        }
    }
}
