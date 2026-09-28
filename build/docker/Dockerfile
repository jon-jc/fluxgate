# Multi-stage build shared by every Fluxgate service.
#
# Pass SERVICE to select which cmd/ package to build:
#   docker build -f build/docker/Dockerfile --build-arg SERVICE=ingest-api .

# ---- build ------------------------------------------------------------------
FROM golang:1.27-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build

ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Copy the manifests first so the dependency layer is cached independently of
# source edits: touching a handler must not re-download the module graph.
COPY go.mod go.sum* ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

# CGO is disabled to produce a static binary that runs on a distroless base
# with no libc at all. -trimpath keeps build paths out of the binary so builds
# are reproducible across machines.
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w \
        -X github.com/jon-jc/fluxgate/internal/version.version=${VERSION} \
        -X github.com/jon-jc/fluxgate/internal/version.commit=${COMMIT} \
        -X github.com/jon-jc/fluxgate/internal/version.date=${BUILD_DATE}" \
      -o /out/service ./cmd/${SERVICE}

# ---- runtime ----------------------------------------------------------------
# Distroless static reduces the runtime surface: no shell or package manager.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS runtime

COPY --from=build /out/service /usr/local/bin/service

# The nonroot user (65532) is baked into the base image.
USER nonroot:nonroot

EXPOSE 8080

ENV HTTP_ADDR=":8080" \
    LOG_FORMAT="json"

ENTRYPOINT ["/usr/local/bin/service"]
