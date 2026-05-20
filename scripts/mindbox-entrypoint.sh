#!/usr/bin/env bash
# mindbox-entrypoint: dispatch to api-rust, e2b-shim, or both. Before
# dispatching, optionally make sure every configured template image is
# present on the host docker daemon (pull from MINDBOX_TEMPLATE_REGISTRY,
# else build locally via template-build).

set -e

if [ -z "${MINDBOX_SKIP_TEMPLATE_ENSURE:-}" ]; then
    /usr/local/bin/ensure-templates || true
fi

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
    *)
        echo "unknown MINDBOX_MODE=$mode (api|shim|both)" >&2
        exit 2
        ;;
esac
