# Eclipse Temurin Java 25 runtime container for Spring Boot 4 Gateway
FROM eclipse-temurin:25-jre-alpine
WORKDIR /app

# Non-root user for security
RUN addgroup -S telemetry && adduser -S telemetry -G telemetry
USER telemetry:telemetry

COPY build/libs/gateway.jar app.jar

EXPOSE 8080

ENV SPRING_PROFILES_ACTIVE=docker \
    JAVA_OPTS="-XX:+UseZGC --add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED -Xmx512m"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
