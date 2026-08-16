package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.api.dto.ProblemDetail;
import com.buildathon.orchestrator.service.DagService;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(DagService.ValidationException.class)
    public ResponseEntity<ProblemDetail> validation(DagService.ValidationException e, HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "Validation failed",
                String.join("; ", e.getErrors()), request);
    }

    @ExceptionHandler(DagService.ConflictException.class)
    public ResponseEntity<ProblemDetail> conflict(DagService.ConflictException e, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, "Conflict", e.getMessage(), request);
    }

    @ExceptionHandler(DagService.NotFoundException.class)
    public ResponseEntity<ProblemDetail> notFound(DagService.NotFoundException e, HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, "Not found", e.getMessage(), request);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ProblemDetail> badRequest(IllegalArgumentException e, HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "Bad request", e.getMessage(), request);
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ProblemDetail> conflictState(IllegalStateException e, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, "State conflict", e.getMessage(), request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProblemDetail> serverError(Exception e, HttpServletRequest request) {
        log.error("Unhandled exception on {}", request.getRequestURI(), e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error",
                "An unexpected error occurred", request);
    }

    private ResponseEntity<ProblemDetail> build(HttpStatus status, String title, String detail, HttpServletRequest request) {
        return ResponseEntity.status(status)
                .body(ProblemDetail.of(status.value(), title, detail, request.getRequestURI()));
    }
}
