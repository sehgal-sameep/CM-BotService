package com.cmbotservice.context;

import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ThreadLocalAccessor;
import jakarta.annotation.PostConstruct;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Hooks;

/**
 * MDC key constants, plus the one-time startup wiring that makes those keys survive across
 * Reactor's thread hops.
 *
 * <p>In a reactive pipeline, MDC (thread-local) does not automatically follow a value written into
 * a Reactor {@link reactor.util.context.Context} — that's exactly what Micrometer's {@link
 * ContextRegistry}/{@link ThreadLocalAccessor} SPI exists to bridge (it's the same mechanism that
 * makes {@code traceId}/{@code spanId} show up in logs from a reactive app). Registering our four
 * keys here means anything upstream that does {@code .contextWrite(ctx ->
 * ctx.put(MdcContext.CORRELATION_ID, id))} (see {@link CorrelationIdFilter}, {@code
 * ChatOrchestrationService}) gets that value restored into MDC automatically at every subsequent
 * operator boundary — request- scoped, not a global/shared {@code ThreadLocal}, and impossible to
 * leak into another request's logs, since Reactor {@code Context} is immutable and
 * per-subscription.
 */
@Component
public final class MdcContext {

  public static final String CORRELATION_ID = "correlationId";
  public static final String TENANT_ID = "tenantId";
  public static final String CASE_ID = "caseId";

  @PostConstruct
  void registerThreadLocalAccessors() {
    ContextRegistry registry = ContextRegistry.getInstance();
    registry.registerThreadLocalAccessor(new MdcKeyAccessor(CORRELATION_ID));
    registry.registerThreadLocalAccessor(new MdcKeyAccessor(TENANT_ID));
    registry.registerThreadLocalAccessor(new MdcKeyAccessor(CASE_ID));
    // Registering accessors alone is not sufficient — Reactor only actually
    // captures/restores Context values around thread hops when automatic context
    // propagation is turned on. Boot *can* enable this itself given
    // io.micrometer:context-propagation on the classpath, but relying on that
    // silently not happening is exactly how tenantId/caseId ended up blank in
    // every log line despite the registration above looking correct — so it's
    // enabled explicitly here rather than assumed.
    Hooks.enableAutomaticContextPropagation();
  }

  private record MdcKeyAccessor(String mdcKey) implements ThreadLocalAccessor<String> {

    @Override
    public Object key() {
      return mdcKey;
    }

    @Override
    public String getValue() {
      return MDC.get(mdcKey);
    }

    @Override
    public void setValue(String value) {
      MDC.put(mdcKey, value);
    }

    @Override
    public void reset() {
      MDC.remove(mdcKey);
    }
  }
}
