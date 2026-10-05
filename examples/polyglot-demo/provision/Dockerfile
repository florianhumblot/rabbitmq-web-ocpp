FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /provision .

FROM scratch
COPY --from=build /provision /provision
ENTRYPOINT ["/provision", "-out", "/out"]
