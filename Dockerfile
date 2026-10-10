ARG GO_VERSION=1.27.1
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/llmgate ./cmd/llmgate

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/llmgate /llmgate
USER 65532:65532
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s CMD ["/llmgate", "healthcheck"]
ENTRYPOINT ["/llmgate"]
