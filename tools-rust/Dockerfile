# Minimal image for the 7-tool dispatcher.
#
#   docker build -t tools-rust -f tools-rust/Dockerfile .
#   docker run --rm -p 8002:8002 -v $PWD/sandboxes:/sandboxes tools-rust
#
# Two stages: build the Rust binary with cargo, then bake it into a
# debian-slim runtime with bash + ripgrep + fd-find available on PATH
# (so tool_bash + future shellouts have the toolchain agents expect).

FROM rust:1-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# Cargo workspace context: copy only what tools-rust needs to build.
COPY Cargo.toml Cargo.lock ./
COPY tools-rust/ ./tools-rust/
# Stub the other workspace members so cargo --workspace resolves but we
# only actually compile tools-rust.
RUN mkdir -p api-rust/src e2b-shim/src worker-rust/src template-builder/src \
    && echo "fn main() {}" | tee api-rust/src/main.rs e2b-shim/src/main.rs worker-rust/src/main.rs template-builder/src/main.rs >/dev/null \
    && for c in api-rust e2b-shim worker-rust template-builder; do \
         printf '[package]\nname = "%s"\nversion = "0.0.0"\nedition = "2021"\n[[bin]]\nname = "%s"\npath = "src/main.rs"\n' "$c" "$c" > "$c/Cargo.toml"; \
       done
RUN cargo build --release -p tools-rust

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates tini bash ripgrep fd-find coreutils findutils \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/fdfind /usr/local/bin/fd
COPY --from=build /src/target/release/tools-rust /usr/local/bin/tools-rust
RUN mkdir -p /sandboxes
ENV TOOLS_PORT=8002 \
    TOOLS_SANDBOX_ROOT=/sandboxes \
    RUST_LOG=info
EXPOSE 8002
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/tools-rust"]
