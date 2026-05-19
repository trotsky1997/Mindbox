# Mindbox controller image — api-rust + e2b-shim + template-build CLI in one.
#
# Modes (MINDBOX_MODE env, default "both"):
#   both — supervises both api-rust on :8000 and e2b-shim on :8001
#   api  — only api-rust  (:8000)
#   shim — only e2b-shim  (:8001)
#
# Build:
#   docker build -t mindbox .
#
# Run (mounts: docker.sock so api-rust can lazy-spawn tools daemon
# containers; templates so the registry knows what's available):
#   docker run --rm \
#     -p 8000:8000 -p 8001:8001 \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     -v $PWD/templates:/opt/inspect-api/templates:ro \
#     mindbox
#
# Shim-only (point at remote api-rust):
#   docker run --rm -p 8001:8001 \
#     -e MINDBOX_MODE=shim \
#     -e E2B_SHIM_UPSTREAM=http://api-host:8000 \
#     mindbox

FROM rust:1-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler libprotobuf-dev pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY proto/ ./proto/
COPY api-rust/ ./api-rust/
COPY e2b-shim/ ./e2b-shim/
COPY template-builder/ ./template-builder/
COPY tools-rust/ ./tools-rust/
RUN cd api-rust && cargo build --release
RUN cd e2b-shim && cargo build --release
RUN cd template-builder && cargo build --release
RUN cd tools-rust && cargo build --release

FROM debian:bookworm-slim
ARG COMPOSE_VERSION=v2.32.4
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates tini bash docker.io curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-compose \
       https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64 \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
COPY --from=build /src/api-rust/target/release/api-rust /usr/local/bin/api-rust
COPY --from=build /src/e2b-shim/target/release/e2b-shim /usr/local/bin/e2b-shim
COPY --from=build /src/template-builder/target/release/template-build /usr/local/bin/template-build
COPY --from=build /src/tools-rust/target/release/tools-rust /opt/inspect-api/tools-rust/target/release/tools-rust
RUN mkdir -p /var/lib/e2b-shim/registry /var/lib/e2b-shim/sandboxes /opt/inspect-api/templates
COPY <<'ENTRY' /usr/local/bin/mindbox-entrypoint
#!/bin/bash
set -e
mode="${MINDBOX_MODE:-both}"
case "$mode" in
  api)  exec /usr/local/bin/api-rust ;;
  shim) exec /usr/local/bin/e2b-shim ;;
  both)
    /usr/local/bin/api-rust &
    api_pid=$!
    : "${E2B_SHIM_UPSTREAM:=http://127.0.0.1:8000}"
    export E2B_SHIM_UPSTREAM
    /usr/local/bin/e2b-shim &
    shim_pid=$!
    trap 'kill -TERM $api_pid $shim_pid 2>/dev/null; wait' INT TERM
    wait -n
    exit_code=$?
    kill -TERM $api_pid $shim_pid 2>/dev/null
    wait 2>/dev/null
    exit $exit_code
    ;;
  *) echo "unknown MINDBOX_MODE=$mode (api|shim|both)" >&2; exit 2 ;;
esac
ENTRY
RUN chmod +x /usr/local/bin/mindbox-entrypoint
EXPOSE 8000 8001
ENV MINDBOX_MODE=both
ENV PORT=8000
ENV E2B_SHIM_PORT=8001
ENV RUST_LOG=info
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/mindbox-entrypoint"]
