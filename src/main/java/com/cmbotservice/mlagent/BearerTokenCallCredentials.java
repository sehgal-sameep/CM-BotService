package com.cmbotservice.mlagent;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.Executor;

/**
 * Per-call gRPC credentials that attach {@code authorization: Bearer <token>} metadata — the
 * standard grpc-java mechanism for request-scoped auth. A new instance is created for each call
 * (via {@code stub.withCallCredentials(...)}, which returns a new stub and leaves the shared one
 * untouched), so a token is never held by the shared channel, stub, or any singleton, and can never
 * be applied to another user's concurrent call.
 *
 * <p>Only metadata is added; the protobuf request is not touched. A server that doesn't read the
 * header simply ignores it.
 *
 * <p>The token is never logged, and {@link #toString()} is redacted so it can't leak through gRPC's
 * own debug output of the call options.
 */
final class BearerTokenCallCredentials extends CallCredentials {

  static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  private final String headerValue;

  /**
   * @param accessToken must be non-blank — callers decide whether a token exists at all, so an
   *     empty {@code "Bearer "} value can never be produced here
   */
  BearerTokenCallCredentials(String accessToken) {
    if (accessToken == null || accessToken.isBlank()) {
      throw new IllegalArgumentException("accessToken must not be blank");
    }
    this.headerValue = "Bearer " + accessToken;
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    try {
      Metadata headers = new Metadata();
      headers.put(AUTHORIZATION, headerValue);
      applier.apply(headers);
    } catch (RuntimeException ex) {
      // Deliberately not chaining ex: its message could echo the header value.
      applier.fail(
          Status.UNAUTHENTICATED.withDescription(
              "Could not attach authorization metadata (" + ex.getClass().getSimpleName() + ")"));
    }
  }

  @Override
  public String toString() {
    return "BearerTokenCallCredentials[<redacted>]";
  }
}
