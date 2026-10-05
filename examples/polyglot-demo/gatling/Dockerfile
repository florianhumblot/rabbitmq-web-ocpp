FROM maven:3.9-eclipse-temurin-21
WORKDIR /gatling
COPY pom.xml .
COPY src src
# Download Gatling and its Maven plugin, and compile, at image build time.
RUN mvn -B -q test-compile gatling:help > /dev/null
ENV GATLING_HEAP=4g
ENTRYPOINT ["sh", "-c", "exec mvn -B gatling:test -Dgatling.heap=${GATLING_HEAP}"]
