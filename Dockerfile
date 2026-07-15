FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app

COPY target/*-runner.jar /app/app.jar

ENV QUARKUS_PROFILE=prod
ENV QUARKUS_HTTP_PORT=8083
ENV QUARKUS_HTTP_HOST=0.0.0.0
ENV QUARKUS_SCHEDULER_ENABLED=true
ENV QUARKUS_DATASOURCE_REACTIVE_URL="postgresql://neondb_owner:npg_2Q0NuTEviwdW@ep-dawn-queen-ath6yyg9-pooler.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
ENV QUARKUS_DATASOURCE_USERNAME=neondb_owner
ENV QUARKUS_DATASOURCE_PASSWORD=npg_2Q0NuTEviwdW

EXPOSE 8083

CMD ["java", "-jar", "/app/app.jar"]