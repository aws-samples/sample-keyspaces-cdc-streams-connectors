FROM amazoncorretto:17-alpine@sha256:858bf22e4155164ccf7ac1373cc9e1a596ceb987b206e0284c266f675cb76e4d
RUN addgroup -S appuser && adduser -S appuser -G appuser
WORKDIR /app
COPY keyspaces-s3-streamer-1.0.0.jar app.jar
RUN chown appuser:appuser app.jar
USER appuser
ENV JAVA_OPTS="-Xmx1536m -Xms512m"
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD ["sh", "-c", "pgrep -f app.jar || exit 1"]
