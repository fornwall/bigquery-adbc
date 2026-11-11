#!/usr/bin/env bash
# Lint and format-check all Python sources, and type-check package and harness code.
set -euo pipefail

if [ "$#" -ne 0 ]; then
  echo "usage: run-python-lint.sh (takes no arguments)" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required; see https://docs.astral.sh/uv/getting-started/installation/" >&2
  exit 1
fi

# The lockfile pins local and CI tools. Editable installation needs no Rust build.
uv sync --quiet --project python --locked

python_tool() {
  uv run --quiet --project python --no-sync "$@"
}

echo ">> ruff check (python/ruff.toml)"
python_tool ruff check --config python/ruff.toml .

echo ">> ruff format --check (python/ruff.toml)"
python_tool ruff format --check --config python/ruff.toml python integration scripts tests tools

echo ">> pyrefly (Python 3.11)"
# Pyrefly's default excludes skip the ../integration includes, without failing, in a checkout
# below a directory the repository ignores, such as an agent worktree in .claude/.
python_tool pyrefly check --config python/pyproject.toml --disable-project-excludes-heuristics=true
