# shellcheck shell=bash
# Build the BigQuery driver cdylib and locate it from Cargo's own report.
# Sourced, not executed.
#
# The artifact may be outside <repository>/target/<profile>: CARGO_TARGET_DIR,
# CARGO_BUILD_TARGET_DIR, CARGO_BUILD_TARGET and their `build` settings in any
# .cargo/config.toml move it, and the dev profile builds into debug/. A guessed
# path could select a stale library left by an earlier build.
#
# The manifest declares only the rlib, so the cdylib is requested here; built
# alone, it also gets the profile's LTO.

# Usage: cargo_build_driver_library <manifest path> [cargo rustc options...]
# Prints the library path, or explains on stderr and returns non-zero.
cargo_build_driver_library() {
    local manifest=$1
    shift
    local messages python=${BIGQUERY_ADBC_PYTHON:-python3}

    if ! command -v "$python" >/dev/null 2>&1; then
        echo "$python is required to read Cargo's artifact report" >&2
        return 1
    fi

    if ! messages=$(cargo rustc --locked --lib --crate-type cdylib --manifest-path "$manifest" \
        --message-format=json-render-diagnostics "$@"); then
        echo "cargo build failed for $manifest" >&2
        return 1
    fi
    printf '%s\n' "$messages" | "$python" -c '
import json
import sys

libraries = []
for line in sys.stdin:
    if not line.startswith("{"):
        continue
    message = json.loads(line)
    if message.get("reason") == "compiler-artifact" and message["target"]["name"] == "adbc_driver_bigquery":
        libraries.extend(path for path in message["filenames"] if path.endswith((".so", ".dylib", ".dll")))
if len(libraries) != 1:
    sys.exit(f"cargo did not report exactly one driver library for {sys.argv[1]}: found {len(libraries)}")
print(libraries[0])
' "$manifest"
}
