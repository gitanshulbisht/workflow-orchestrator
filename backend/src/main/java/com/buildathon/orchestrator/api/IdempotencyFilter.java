package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.config.OrchestratorProperties;
import com.buildathon.orchestrator.persistence.IdempotencyRecordEntity;
import com.buildathon.orchestrator.persistence.IdempotencyRecordRepository;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;

/**
 * Honors the Idempotency-Key header for mutating requests.
 *
 * Concurrency-safe design: on the first sight of a key we INSERT a PENDING
 * record before running the handler. The unique primary key on the record
 * table acts as a mutex — a concurrent duplicate insert fails and that
 * request gets a 409. After the handler completes we UPDATE the record with
 * the status and response body. Later replays skip the handler entirely and
 * return the stored response.
 */
@Component
public class IdempotencyFilter extends OncePerRequestFilter {

    public static final String HEADER = "Idempotency-Key";

    private static final Logger log = LoggerFactory.getLogger(IdempotencyFilter.class);

    private final IdempotencyRecordRepository repository;
    private final OrchestratorProperties properties;
    private final TransactionTemplate transactionTemplate;

    public IdempotencyFilter(IdempotencyRecordRepository repository, OrchestratorProperties properties,
                             TransactionTemplate transactionTemplate) {
        this.repository = repository;
        this.properties = properties;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String key = request.getHeader(HEADER);
        if (key == null || key.isBlank()) {
            return true;
        }
        String method = request.getMethod();
        return !(method.equals("POST") || method.equals("PUT") || method.equals("PATCH") || method.equals("DELETE"));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String key = request.getHeader(HEADER);
        String body = readBody(request);
        String hash = sha256(request.getMethod() + "|" + request.getRequestURI() + "|" + body);

        // 1. Try to claim the key by inserting a PENDING record. The unique key
        //    constraint acts as a mutex; a duplicate insert rolls back cleanly
        //    and we treat the key as already claimed.
        boolean claimed;
        try {
            claimed = Boolean.TRUE.equals(transactionTemplate.execute(status -> {
                repository.saveAndFlush(new IdempotencyRecordEntity(
                        key, hash, 0, null,
                        Instant.now().plusSeconds(properties.idempotency().ttlMinutes() * 60)));
                return true;
            }));
        } catch (DataIntegrityViolationException e) {
            claimed = false;
        }
        if (!claimed) {
            // 2. Key already known: replay the stored response or reject a conflicting hash.
            var existing = repository.findById(key);
            if (existing.isEmpty()) {
                // Record expired between insert-fail and read; treat as a conflict.
                response.setStatus(HttpServletResponse.SC_CONFLICT);
                response.setContentType("application/json");
                response.getWriter().write("""
                        {"type":"about:blank","title":"Idempotency key in use","status":409,"detail":"A request with this Idempotency-Key is being processed or was recently completed."}
                        """);
                return;
            }
            IdempotencyRecordEntity record = existing.get();
            if (!record.getRequestHash().equals(hash)) {
                log.warn("Idempotency key {} reused with a different request body", key);
                response.setStatus(422);
                response.setContentType("application/json");
                response.getWriter().write("""
                        {"type":"about:blank","title":"Idempotency key conflict","status":422,"detail":"The Idempotency-Key was already used with a different request body."}
                        """);
                return;
            }
            if (record.getStatusCode() == 0) {
                // Still being processed by the original request.
                response.setStatus(HttpServletResponse.SC_CONFLICT);
                response.setContentType("application/json");
                response.getWriter().write("""
                        {"type":"about:blank","title":"Idempotency key in use","status":409,"detail":"A request with this Idempotency-Key is being processed."}
                        """);
                return;
            }
            response.setStatus(record.getStatusCode());
            response.setContentType("application/json");
            if (record.getResponseBody() != null) {
                response.getWriter().write(record.getResponseBody());
            }
            return;
        }

        // 3. First sight of the key: run the handler on a request replaying the body.
        HttpServletRequest bodyReplayingRequest = new BodyReplayRequestWrapper(request, body);
        ContentCachingResponseWrapper wrappedResponse = new ContentCachingResponseWrapper(response);
        try {
            chain.doFilter(bodyReplayingRequest, wrappedResponse);
        } catch (Exception e) {
            // Handler failed: remove the PENDING record so the key can be retried.
            transactionTemplate.executeWithoutResult(status -> repository.deleteById(key));
            throw e;
        }

        String responseBody = new String(wrappedResponse.getContentAsByteArray(), StandardCharsets.UTF_8);
        transactionTemplate.executeWithoutResult(status -> {
            var record = repository.findById(key).orElseThrow();
            record.complete(hash, wrappedResponse.getStatus(), responseBody.isEmpty() ? null : responseBody);
            repository.save(record);
        });
        wrappedResponse.copyBodyToResponse();
    }

    private String readBody(ServletRequest request) throws IOException {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8))) {
            char[] buffer = new char[4096];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                builder.append(buffer, 0, read);
            }
        }
        return builder.toString();
    }

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class BodyReplayRequestWrapper extends HttpServletRequestWrapper {

        private final byte[] body;

        BodyReplayRequestWrapper(HttpServletRequest request, String body) {
            super(request);
            this.body = body.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public ServletInputStream getInputStream() {
            ByteArrayInputStream input = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override
                public boolean isFinished() {
                    return input.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener readListener) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int read() {
                    return input.read();
                }
            };
        }

        @Override
        public BufferedReader getReader() {
            return new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
        }

        @Override
        public int getContentLength() {
            return body.length;
        }

        @Override
        public long getContentLengthLong() {
            return body.length;
        }
    }
}
