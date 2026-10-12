# syntax=docker/dockerfile:1

ARG GO_IMAGE=golang:1.24.13-alpine3.22
ARG RUNTIME_IMAGE=alpine:3.22.6

FROM ${GO_IMAGE} AS builder

ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z

RUN apk add --no-cache git

WORKDIR /src

RUN git clone --depth 1 --branch "${MINIO_RELEASE}" \
        https://github.com/minio/minio.git /src/minio \
    && cd /src/minio \
    && LDFLAGS="$(go run buildscripts/gen-ldflags.go)" \
    && CGO_ENABLED=0 go build \
        -tags kqueue \
        -trimpath \
        -ldflags "${LDFLAGS}" \
        -o /out/minio .

RUN git clone --depth 1 --branch "${MC_RELEASE}" \
        https://github.com/minio/mc.git /src/mc \
    && cd /src/mc \
    && LDFLAGS="$(go run buildscripts/gen-ldflags.go)" \
    && CGO_ENABLED=0 go build \
        -tags kqueue \
        -trimpath \
        -ldflags "${LDFLAGS}" \
        -o /out/mc .

RUN /out/minio --version && /out/mc --version

FROM ${RUNTIME_IMAGE}

ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z

LABEL org.opencontainers.image.title="WABA MinIO" \
      org.opencontainers.image.description="MinIO server and client built from pinned official source releases" \
      org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.version="${MINIO_RELEASE}" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later" \
      io.waba.mc.version="${MC_RELEASE}"

RUN apk add --no-cache ca-certificates curl \
    && addgroup -S -g 10001 minio \
    && adduser -S -D -H -u 10001 -G minio minio \
    && mkdir -p /data /licenses \
    && chown -R minio:minio /data

COPY --from=builder /out/minio /usr/local/bin/minio
COPY --from=builder /out/mc /usr/local/bin/mc
COPY --from=builder /src/minio/LICENSE /licenses/MINIO_LICENSE
COPY --from=builder /src/minio/CREDITS /licenses/MINIO_CREDITS
COPY --from=builder /src/mc/LICENSE /licenses/MC_LICENSE
COPY --from=builder /src/mc/CREDITS /licenses/MC_CREDITS

ENV HOME=/tmp \
    MC_CONFIG_DIR=/tmp/.mc

USER minio:minio

EXPOSE 9000 9001
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--address", ":9000", "--console-address", ":9001"]
