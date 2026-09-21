package com.cmbotservice.common;

/**
 * The ML Agent explicitly rejected the request for a non-transient reason — bad/expired service
 * credential (~401), tenant not permitted (~403), case not found in that tenant (~404), or a
 * malformed request/missing required context (~422), expressed as the corresponding gRPC status
 * code on the initial {@code Chat} call, before any event is streamed. Never retryable: the agent
 * understood the request and said no, so retrying unchanged would just get the same answer again.
 *
 * <p>Carries its own {@link ErrorCode} rather than always mapping to one fixed code, since the
 * right code genuinely differs by which status this wraps (see {@code GrpcMlAgentClient} for the
 * mapping).
 */
public class MlAgentRejectedException extends MlAgentException {

  private final ErrorCode errorCode;

  public MlAgentRejectedException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public ErrorCode errorCode() {
    return errorCode;
  }
}
