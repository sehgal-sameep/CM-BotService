package com.cmbotservice.config;

import com.cmbotservice.context.RequestHeaders;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.parameters.Parameter;
import org.springdoc.core.customizers.OperationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.HandlerMethod;

/**
 * API metadata plus a global {@link OperationCustomizer} that documents the
 * cross-cutting headers ({@code X-Correlation-Id}, {@code X-User-Id}, {@code
 * X-Tenant-Id}, {@code X-Org-Id}) on every operation, rather than repeating
 * {@code @Parameter} annotations on each controller method.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI cmBotServiceOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Case Manager Chatbot Backend")
                .version("v1")
                .description("Stateless, non-blocking (Spring WebFlux/Reactor) integration/orchestration "
                        + "layer between the Case Manager chatbot capability and the external ML/AI "
                        + "Agent. This service holds no conversation state between requests — it "
                        + "validates each request, forwards it to the ML Agent through a circuit "
                        + "breaker, bulkhead, and bounded retry, and streams the response back over "
                        + "SSE. Conversation memory (if any) is owned entirely by the ML Agent; this "
                        + "backend does not implement any ML/LLM logic itself. The ML Agent is "
                        + "currently a configurable mock — see the 'Chat' endpoint description for the "
                        + "scenario-simulation keywords.")
                .contact(new Contact().name("Case Manager Platform Team")));
    }

    @Bean
    public OperationCustomizer commonHeaderParametersCustomizer() {
        return this::addCommonHeaders;
    }

    private Operation addCommonHeaders(Operation operation, HandlerMethod handlerMethod) {
        operation.addParametersItem(new Parameter()
                .in("header")
                .name(RequestHeaders.CORRELATION_ID)
                .required(false)
                .description("Caller-supplied correlation ID for tracing this interaction across "
                        + "logs; a new one is generated and echoed back on the response if omitted.")
                .example("3f2c9e1a-1234-4c56-9abc-1234567890ab"));
        operation.addParametersItem(new Parameter()
                .in("header")
                .name(RequestHeaders.USER_ID)
                .required(false)
                .description("Placeholder analyst identity header, standing in for a real "
                        + "authentication principal (JWT/session) until the platform's auth "
                        + "mechanism is wired in.")
                .example("analyst-1"));
        operation.addParametersItem(new Parameter()
                .in("header")
                .name(RequestHeaders.TENANT_ID)
                .required(true)
                .description("Tenant identifier. Required on every request — a blank or missing "
                        + "value is rejected with a 400 validation error.")
                .example("tenant-123"));
        operation.addParametersItem(new Parameter()
                .in("header")
                .name(RequestHeaders.ORGANIZATION_ID)
                .required(true)
                .description("Organization identifier, forwarded to the ML Agent's request "
                        + "context. Required on every request — a blank or missing value is "
                        + "rejected with a 400 validation error.")
                .example("org-123"));
        return operation;
    }
}
