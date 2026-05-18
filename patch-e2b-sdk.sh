#!/usr/bin/env bash
# Patches the locally-installed e2b SDK to remove debug-mode no-op short-circuits
# in `kill()` and `set_timeout()`. Required when running against the e2b-shim
# (debug=True is used to route to localhost, but the SDK upstream considers
# debug mode local-only and skips cleanup/timeout calls).
#
# Re-run after `pip install --upgrade e2b`.
set -euo pipefail

py=$(python3 -c "import e2b, os; print(os.path.dirname(e2b.__file__))")
if [ -z "$py" ]; then echo "e2b SDK not found"; exit 1; fi

echo "patching: $py"
for f in "$py/sandbox_sync/sandbox_api.py" "$py/sandbox_async/sandbox_api.py"; do
  python3 - "$f" <<'PYEOF'
import sys, re
path = sys.argv[1]
with open(path) as fp: src = fp.read()
original = src
# Drop the "Skip killing/setting timeout" debug short-circuits
patterns = [
    (r'        if config\.debug:\n            # Skip killing the sandbox in debug mode\n            return True\n\n', ''),
    (r'        if config\.debug:\n            # Skip setting (?:the )?timeout in debug mode\n            return\n\n', ''),
]
for pat, repl in patterns:
    src = re.sub(pat, repl, src)
if src != original:
    with open(path, 'w') as fp: fp.write(src)
    print(f"  patched {path}")
else:
    print(f"  (no changes needed) {path}")
PYEOF
done

# Bust the compiled .pyc cache so the patch takes effect
find "$py" -name "*.pyc" -delete
echo "done"
