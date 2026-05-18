# Multi-stage build for api-rust.
#
# Build stage uses rust:slim with protoc; runtime is debian:slim with just the binary.
# Note: api-rust manages docker containers via /var/run/docker.sock and bind-mounts a
# shared socket dir into worker containers. To run THIS image in docker, you must:
#   1. Bind-mount the host docker socket:  -v /var/run/docker.sock:/var/run/docker.sock
#   2. Share /opt/inspect-api/sockets/ between this container and the worker
#      containers it spawns (same host path on both sides).
#   3. Build the inspect-tpl-* template images on the host first via template-builder.
#
# Build:
#   docker build -f api-rust/Dockerfile -t mindbox-api .
# Run (example):
#   docker run --rm \
#     -p 8000:8000 \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     -v /opt/inspect-api/sockets:/opt/inspect-api/sockets \
#     -v $PWD/templates:/opt/inspect-api/templates:ro \
#     -e INSPECT_API_TEMPLATES_DIR=/opt/inspect-api/templates \
#     mindbox-api

FROM rust:1-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler libprotobuf-dev pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY proto/ ./proto/
COPY api-rust/ ./api-rust/
WORKDIR /src/api-rust
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/api-rust/target/release/api-rust /usr/local/bin/api-rust
EXPOSE 8000
ENV PORT=8000
ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/api-rust"]
