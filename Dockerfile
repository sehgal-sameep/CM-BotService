#####################################################################
# Stage 1: build the executable jar
#####################################################################
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace

# Wrapper + POM first, in their own layer: dependency resolution is only re-run when
# pom.xml (or the wrapper itself) actually changes, not on every source edit below.
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw \
    && ./mvnw -B -q dependency:go-offline
# NOTE: go-offline resolves normal dependencies/plugins, but NOT the per-OS
# protoc/protoc-gen-grpc-java binaries protobuf-maven-plugin downloads via
# os-maven-plugin during generate-sources (see pom.xml) — those are fetched on the
# next RUN instead, the first time it actually executes that plugin goal. Both RUNs
# therefore need network access during `docker build`; nothing at container *runtime*
# needs it.

# Now the real sources.
COPY src ./src
RUN ./mvnw -B -q clean package -DskipTests \
    && JAR_FILE=$(find target -maxdepth 1 -name '*.jar' ! -name '*.jar.original') \
    && cp "$JAR_FILE" app.jar

#####################################################################
# Stage 2: minimal runtime image
#####################################################################
FROM eclipse-temurin:21-jre-jammy AS runtime

# curl is only used by HEALTHCHECK below; eclipse-temurin's base image doesn't ship it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Runs as an unprivileged user — this app never needs root at runtime.
RUN groupadd --system spring && useradd --system --gid spring --no-create-home spring

WORKDIR /app
COPY --from=build /workspace/app.jar ./app.jar
RUN chown spring:spring /app/app.jar
USER spring

EXPOSE 8080

# Override at `docker run`/orchestrator level, e.g. -e JAVA_OPTS="-Xmx512m -Xms256m".
# Left blank by default: the JVM's container-aware ergonomics (default since JDK 10+)
# already size heap/GC from the container's own cgroup memory limit without this.
ENV JAVA_OPTS=""

# Every property in application.yml is overridable via Spring Boot's relaxed env-var
# binding — no code/image change needed to point this at real infrastructure, e.g.:
#   -e ML_AGENT_MODE=grpc -e ML_AGENT_GRPC_HOST=ml-agent -e ML_AGENT_GRPC_PORT=9090
#   -e CHATBOT_SECURITY_MODE=BFF_SESSION -e CHATBOT_SECURITY_REDIS_HOST=redis
# See README.md's "Configuration" and "Authentication" sections for the full property list.

HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8080/actuator/health/liveness || exit 1

# `exec`, not a bare shell command, so `java` runs as PID 1 and receives SIGTERM
# directly — required for spring.lifecycle's graceful shutdown (server.shutdown:
# graceful) to actually get a chance to drain in-flight requests/SSE streams before
# the container is killed, instead of the shell eating the signal.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
