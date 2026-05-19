# Combined image: builds api-rust + e2b-shim, runs either one or both.
#
# MINDBOX_MODE selects what to run:
#   both  (default) — supervises api-rust on :8000 and e2b-shim on :8001 in one container
#   api             — only api-rust (:8000)
#   shim            — only e2b-shim (:8001)
#
# Build:
#   docker build -t mindbox .
#
# Run both in one container:
#   docker run --rm \
#     -p 8000:8000 -p 8001:8001 \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     -v /opt/inspect-api/sockets:/opt/inspect-api/sockets \
#     -v $PWD/templates:/opt/inspect-api/templates:ro \
#     -v mindbox-shim-state:/var/lib/e2b-shim \
#     -e INSPECT_API_TEMPLATES_DIR=/opt/inspect-api/templates \
#     mindbox
#
# Run only the shim, pointing at an external api-rust host:
#   docker run --rm -p 8001:8001 \
#     -e MINDBOX_MODE=shim \
#     -e E2B_SHIM_UPSTREAM=http://api-host:8000 \
#     mindbox

# IMPORTANT: this builder ships Python 3.11. The worker-rust binary built
# here links to libpython3.11.so via PyO3. Any template image
# (templates/*/template.toml `base_image`) MUST also ship Python 3.11 or
# the worker container will die on startup with
# "libpython3.11.so.1.0: cannot open shared object file".
# If you upgrade this base image, audit every template.toml `base_image`.
FROM rust:1-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler libprotobuf-dev pkg-config libssl-dev \
        python3-dev libpython3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY proto/ ./proto/
COPY api-rust/ ./api-rust/
COPY e2b-shim/ ./e2b-shim/
COPY template-builder/ ./template-builder/
COPY worker-rust/ ./worker-rust/
COPY tools-rust/ ./tools-rust/
RUN cd api-rust && cargo build --release
RUN cd e2b-shim && cargo build --release
RUN cd template-builder && cargo build --release
RUN cd worker-rust && cargo build --release
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
# template-build builds template images by copying the worker binary +
# generated pb2.py into a temp build context. Keep them at the same paths
# the binary expects on bare-metal install (/opt/inspect-api/...).
COPY --from=build /src/worker-rust/target/release/worker-rust /opt/inspect-api/worker-rust/target/release/worker-rust
# Bundle tools-rust the same way: template-builder picks it up at this path
# when generating a kind="tools" template image.
COPY --from=build /src/tools-rust/target/release/tools-rust /opt/inspect-api/tools-rust/target/release/tools-rust
COPY proto/inspect_pb2.py /opt/inspect-api/proto/inspect_pb2.py
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
    # Default shim → local api when running both in one container
    : "${E2B_SHIM_UPSTREAM:=http://127.0.0.1:8000}"
    export E2B_SHIM_UPSTREAM
    /usr/local/bin/e2b-shim &
    shim_pid=$!
    trap 'kill -TERM $api_pid $shim_pid 2>/dev/null; wait' INT TERM
    # Exit when either dies — let the orchestrator restart the container.
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
