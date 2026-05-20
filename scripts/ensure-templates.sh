#!/usr/bin/env bash
# ensure-templates: make sure every configured template has its
# inspect-tpl-tools-<name>:latest image present on the host docker daemon.
#
# Per template in $INSPECT_API_TEMPLATES_DIR (default /opt/inspect-api/templates):
#
#   1. Skip when `docker image inspect inspect-tpl-tools-<name>:latest`
#      already succeeds (cheap restart fast-path).
#   2. If MINDBOX_TEMPLATE_REGISTRY is set, try `docker pull` from
#      "${MINDBOX_TEMPLATE_REGISTRY}/tpl-<name>:${MINDBOX_TEMPLATE_TAG:-latest}"
#      and re-tag locally on success.
#   3. Otherwise fall back to in-image template-build to build it from source
#      against /var/run/docker.sock.
#
# Failures are warnings; the controller still starts. Operators that do not
# need pre-warmed templates (e.g. shim-only) can set
# MINDBOX_SKIP_TEMPLATE_ENSURE=1 to skip this whole step.

set -u

TEMPLATES_DIR="${INSPECT_API_TEMPLATES_DIR:-/opt/inspect-api/templates}"
REGISTRY="${MINDBOX_TEMPLATE_REGISTRY:-}"
TAG="${MINDBOX_TEMPLATE_TAG:-latest}"

if [ ! -d "$TEMPLATES_DIR" ]; then
    echo "[mindbox] templates dir $TEMPLATES_DIR not found, skipping ensure" >&2
    exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "[mindbox] docker CLI not found, skipping template ensure" >&2
    exit 0
fi

if ! docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
    echo "[mindbox] docker daemon unreachable, skipping template ensure" >&2
    exit 0
fi

shopt -s nullglob
present=0
pulled=0
built=0
failed=0
for toml in "$TEMPLATES_DIR"/*/template.toml; do
    name="$(basename "$(dirname "$toml")")"
    tag="inspect-tpl-tools-${name}:latest"

    if docker image inspect "$tag" >/dev/null 2>&1; then
        present=$((present + 1))
        continue
    fi

    if [ -n "$REGISTRY" ]; then
        remote="${REGISTRY%/}/tpl-${name}:${TAG}"
        if docker pull "$remote" >/dev/null 2>&1; then
            docker tag "$remote" "$tag" >/dev/null 2>&1 || true
            echo "[mindbox] pulled $remote → $tag" >&2
            pulled=$((pulled + 1))
            continue
        fi
        echo "[mindbox] pull $remote failed, will try local build" >&2
    fi

    if /usr/local/bin/template-build --root /opt/inspect-api "$name" >&2; then
        echo "[mindbox] built $tag from source" >&2
        built=$((built + 1))
    else
        echo "[mindbox] WARN: could not ensure $tag (no registry hit, build failed)" >&2
        failed=$((failed + 1))
    fi
done

echo "[mindbox] templates: present=$present pulled=$pulled built=$built failed=$failed" >&2
