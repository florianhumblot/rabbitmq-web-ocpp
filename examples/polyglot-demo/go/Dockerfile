# Build context: examples/polyglot-demo (the dashboard page is shared).
# One image, three services: ocpp-worker, command-api and dashboard.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go/go.mod go/go.sum ./
RUN go mod download
COPY go/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
COPY --from=build /out/ /usr/local/bin/
COPY dashboard /app/dashboard
ENV STATIC_DIR=/app/dashboard
EXPOSE 8080
