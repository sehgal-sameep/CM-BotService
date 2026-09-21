package com.cmbotservice.web.dto;

import com.cmbotservice.common.ErrorCode;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;

@Schema(description = "Standard error body returned by all REST endpoints on failure.")
public record ErrorResponse(
    ErrorCode errorCode, String message, String correlationId, Instant timestamp) {
  public static ErrorResponse of(ErrorCode errorCode, String message, String correlationId) {
    return new ErrorResponse(errorCode, message, correlationId, Instant.now());
  }
}
