package com.cmbotservice.web.advice;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.LogSanitizer;
import com.cmbotservice.context.CorrelationIdFilter;
import com.cmbotservice.web.dto.ErrorResponse;
import jakarta.validation.ConstraintViolationException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

/**
 * Central mapping from pre-stream validation/routing exceptions to HTTP responses. This only ever
 * runs <em>before</em> an SSE response has started (request-body validation, an unmatched route) —
 * once {@code ChatOrchestrationService} starts streaming, the response is already committed at 200,
 * so any failure after that point becomes an {@code error} SSE event instead (see that class),
 * never a handler here.
 *
 * <p>This service is a stateless orchestrator with no stored resources to look up, so there is no
 * ownership/lifecycle exception category — only request-shape validation, routing, and a catch-all
 * for the truly unexpected.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

  @ExceptionHandler(WebExchangeBindException.class)
  public ResponseEntity<ErrorResponse> handleValidation(
      WebExchangeBindException ex, ServerWebExchange exchange) {
    String message =
        ex.getBindingResult().getFieldErrors().stream()
            .map(fieldError -> fieldError.getField() + ": " + fieldError.getDefaultMessage())
            .collect(Collectors.joining("; "));
    log.warn("REQUEST_VALIDATION_FAILED {} status=400 errors=[{}]", describe(exchange), message);
    return badRequest(ErrorCode.VALIDATION_ERROR, message, exchange);
  }

  @ExceptionHandler(ConstraintViolationException.class)
  public ResponseEntity<ErrorResponse> handleConstraintViolation(
      ConstraintViolationException ex, ServerWebExchange exchange) {
    log.warn(
        "REQUEST_CONSTRAINT_VIOLATION {} status=400 errors=[{}]",
        describe(exchange),
        ex.getMessage());
    return badRequest(ErrorCode.VALIDATION_ERROR, ex.getMessage(), exchange);
  }

  /**
   * Covers an unmatched route (WebFlux's own routing exception is a {@code ResponseStatusException}
   * subtype) and any other framework-thrown status-carrying exception, generically — deliberately
   * not narrowed to one specific subclass, so a future Spring-thrown {@code
   * ResponseStatusException} of a kind we haven't seen yet still gets its real status code instead
   * of being swallowed by {@link #handleUnexpected} and misreported as a 500.
   */
  @ExceptionHandler(ResponseStatusException.class)
  public ResponseEntity<ErrorResponse> handleResponseStatusException(
      ResponseStatusException ex, ServerWebExchange exchange) {
    HttpStatusCode status = ex.getStatusCode();
    ErrorCode code = status.value() == 404 ? ErrorCode.NOT_FOUND : ErrorCode.VALIDATION_ERROR;
    String message =
        status.value() == 404
            ? "No such endpoint."
            : (ex.getReason() != null ? ex.getReason() : "Invalid request.");
    log.warn(
        "REQUEST_REJECTED {} status={} reason='{}'", describe(exchange), status.value(), message);
    return ResponseEntity.status(status)
        .body(ErrorResponse.of(code, message, correlationId(exchange)));
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex, ServerWebExchange exchange) {
    log.error(
        "REQUEST_FAILED_UNEXPECTEDLY {} status=500 cause=[{}]",
        describe(exchange),
        LogSanitizer.causeChain(ex),
        ex);
    return ResponseEntity.internalServerError()
        .body(
            ErrorResponse.of(
                ErrorCode.INTERNAL_ERROR,
                "An unexpected error occurred.",
                correlationId(exchange)));
  }

  private static ResponseEntity<ErrorResponse> badRequest(
      ErrorCode code, String message, ServerWebExchange exchange) {
    return ResponseEntity.badRequest()
        .body(ErrorResponse.of(code, message, correlationId(exchange)));
  }

  private static String describe(ServerWebExchange exchange) {
    return "method="
        + exchange.getRequest().getMethod().name()
        + " path="
        + exchange.getRequest().getPath().value();
  }

  private static String correlationId(ServerWebExchange exchange) {
    return exchange.getAttribute(CorrelationIdFilter.CORRELATION_ID_ATTRIBUTE);
  }
}
