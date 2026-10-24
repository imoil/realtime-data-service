# Eclipse Temurin Java 21 runtime container for Spring Boot Gateway
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Non-root user for security
RUN addgroup -S telemetry && adduser -S telemetry -G telemetry
USER telemetry:telemetry

COPY build/libs/gateway.jar app.jar

EXPOSE 8080

ENV SPRING_PROFILES_ACTIVE=docker \
    JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xmx512m"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
