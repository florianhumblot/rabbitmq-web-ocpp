# Build context: examples/polyglot-demo (the dashboard page and the registry
# script are shared with the other implementations).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src/java
COPY java/pom.xml .
RUN mvn -B -q dependency:go-offline
COPY dashboard /src/dashboard
COPY valkey/registry.lua /src/valkey/registry.lua
COPY java/src src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
COPY --from=build /src/java/target/csms-java-1.0.0.jar /app/csms.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/csms.jar"]
