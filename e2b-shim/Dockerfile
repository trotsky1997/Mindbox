# Multi-stage build for e2b-shim.
#
# Drop-in E2B-compatible API server. Talks HTTP to api-rust upstream.
# Persists sandbox metadata to /var/lib/e2b-shim/ — bind-mount or volume that path
# if you want sandbox state to survive container restarts.
#
# Build:
#   docker build -f e2b-shim/Dockerfile -t mindbox-shim .
# Run:
#   docker run --rm \
#     -p 8001:8001 \
#     -e E2B_SHIM_UPSTREAM=http://api-host:8000 \
#     -v /var/lib/e2b-shim:/var/lib/e2b-shim \
#     mindbox-shim

FROM rust:1-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler libprotobuf-dev pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY proto/ ./proto/
COPY e2b-shim/ ./e2b-shim/
WORKDIR /src/e2b-shim
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/e2b-shim/target/release/e2b-shim /usr/local/bin/e2b-shim
RUN mkdir -p /var/lib/e2b-shim/registry /var/lib/e2b-shim/sandboxes
EXPOSE 8001
ENV E2B_SHIM_PORT=8001
ENV E2B_SHIM_UPSTREAM=http://127.0.0.1:8000
ENTRYPOINT ["/usr/local/bin/e2b-shim"]
