package com.cmbotservice.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ResponseStatusException;

class HeaderBasedRequestContextResolverTest {

  private final HeaderBasedRequestContextResolver resolver =
      new HeaderBasedRequestContextResolver();

  private static MockServerWebExchange exchange(MockServerHttpRequest.BaseBuilder<?> request) {
    return MockServerWebExchange.from(request);
  }

  @Test
  void suppliedOrganizationIdHeader_isResolvedAsIs() {
    MockServerWebExchange exchange =
        exchange(
            MockServerHttpRequest.post("/")
                .header(RequestHeaders.TENANT_ID, "tenant-1")
                .header(RequestHeaders.ORGANIZATION_ID, "org-1"));

    assertThat(resolver.resolve(exchange, "case-1").organization()).isEqualTo("org-1");
  }

  @Test
  void missingOrganizationIdHeader_resolvesToNull() {
    MockServerWebExchange exchange =
        exchange(MockServerHttpRequest.post("/").header(RequestHeaders.TENANT_ID, "tenant-1"));

    assertThat(resolver.resolve(exchange, "case-1").organization()).isNull();
  }

  @Test
  void blankOrganizationIdHeader_resolvesToNull() {
    MockServerWebExchange exchange =
        exchange(
            MockServerHttpRequest.post("/")
                .header(RequestHeaders.TENANT_ID, "tenant-1")
                .header(RequestHeaders.ORGANIZATION_ID, "  "));

    assertThat(resolver.resolve(exchange, "case-1").organization()).isNull();
  }

  @Test
  void missingTenantIdHeader_isStillRejected() {
    MockServerWebExchange exchange =
        exchange(MockServerHttpRequest.post("/").header(RequestHeaders.ORGANIZATION_ID, "org-1"));

    assertThatThrownBy(() -> resolver.resolve(exchange, "case-1"))
        .isInstanceOf(ResponseStatusException.class);
  }
}
