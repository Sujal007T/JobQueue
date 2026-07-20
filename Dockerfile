# ---------------------------------------------------------------
# Stage 1: Build — uses Maven to compile and package the app
# ---------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-17-alpine AS build

WORKDIR /app

# Copy pom.xml first for dependency caching
COPY pom.xml .

# Download dependencies (cached unless pom.xml changes)
RUN mvn dependency:go-offline -B

# Copy source code and build
COPY src ./src
RUN mvn package -DskipTests -B

# ---------------------------------------------------------------
# Stage 2: Run — minimal JRE image with just the fat JAR
# ---------------------------------------------------------------
FROM eclipse-temurin:17-jre-alpine AS runtime

WORKDIR /app

# Create a non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=build /app/target/*.jar app.jar

RUN chown -R appuser:appgroup /app
USER appuser

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
