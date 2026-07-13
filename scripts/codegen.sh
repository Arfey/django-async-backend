#!/usr/bin/env bash
# Regenerate the codemon files: codemon + black + isort.
# --check: also fail if the result differs from the committed files.
# Run inside the project env: poetry run scripts/codegen.sh [--check]
set -euo pipefail
cd "$(dirname "$0")/.."

check=false
if [ "${1:-}" = "--check" ]; then
  check=true
elif [ -n "${1:-}" ]; then
  echo "unknown argument: $1 (usage: scripts/codegen.sh [--check])" >&2
  exit 2
fi

# The generated file list comes from the codemon configs.
files_txt=$(python - <<'PY'
from codemon.utils import (
    get_configs,
    load_config,
)

for path in sorted(get_configs()):
    print("django_async_backend/" + load_config(path).pathname)
PY
)
files=()
while IFS= read -r f; do files+=("$f"); done <<< "$files_txt"
if [ "${#files[@]}" -eq 0 ]; then
  echo "no codemon configs found" >&2
  exit 1
fi

# git status (unlike git diff) also catches never-committed generated files.
if $check && [ -n "$(git status --porcelain -- "${files[@]}")" ]; then
  echo "generated files have local changes; commit or restore them first" >&2
  exit 1
fi

python -m codemon
python -m black -q "${files[@]}"
python -m isort -q "${files[@]}"
python -m black --check -q "${files[@]}"

if $check; then
  if [ -z "$(git status --porcelain -- "${files[@]}")" ]; then
    echo "✅ committed files match codemon output"
  else
    git --no-pager diff -- "${files[@]}"
    git status --short -- "${files[@]}" >&2
    echo "❌ committed files do not match codemon output (diff above)." >&2
    echo "   Commit the regenerated files, or fix codemon/config." >&2
    exit 1
  fi
fi
