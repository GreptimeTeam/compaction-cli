# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.3-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY skills ./skills
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w -X main.version=$VERSION -X main.commit=$COMMIT" -o /out/greptime-compaction-cli .

FROM scratch
LABEL org.opencontainers.image.source="https://github.com/GreptimeTeam/compaction-cli"
LABEL org.opencontainers.image.licenses="Apache-2.0"
COPY --from=builder /out/greptime-compaction-cli /usr/local/bin/greptime-compaction-cli
COPY skills /opt/compaction-cli/skills
COPY LICENSE /opt/compaction-cli/LICENSE
USER 65532:65532
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/greptime-compaction-cli"]
CMD ["--help"]
