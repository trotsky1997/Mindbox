#!/usr/bin/env bash
# Save a local docker image as a tarball and upload to TOS for archival.
# Loads config from .env at repo root (or current dir).
#
# Usage:
#   ./scripts/backup-image-to-tos.sh inspect-tpl-default:latest
#
# Lands at: tos://$TOS_BUCKET/harbor/containers/<sanitized-tag>.tar
# Requires: docker, aws CLI (v2 recommended).

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <image:tag>" >&2
  exit 2
fi
IMAGE="$1"

# Load .env if present (in cwd or repo root).
for env_path in ./.env ./../.env; do
  if [ -f "$env_path" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$env_path"
    set +a
    break
  fi
done

: "${TOS_BUCKET:?TOS_BUCKET not set; check .env}"
: "${TOS_S3_ENDPOINT:?TOS_S3_ENDPOINT not set}"
: "${TOS_ACCESS_KEY:?TOS_ACCESS_KEY not set}"
: "${TOS_SECRET_KEY:?TOS_SECRET_KEY not set}"
: "${TOS_REGION:=cn-beijing}"

# Sanitize tag → object key.
SAFE=$(echo "$IMAGE" | tr '/:' '__')
KEY="harbor/containers/${SAFE}.tar"
URL="s3://${TOS_BUCKET}/${KEY}"

echo "[backup] image=$IMAGE → $URL"
echo "[backup] streaming docker save → tos ..."

export AWS_ACCESS_KEY_ID="$TOS_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$TOS_SECRET_KEY"
export AWS_DEFAULT_REGION="$TOS_REGION"

docker save "$IMAGE" | aws s3 cp \
  --endpoint-url "$TOS_S3_ENDPOINT" \
  - "$URL"

echo "[backup] done: $URL"
