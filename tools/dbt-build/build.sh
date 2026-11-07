#!/usr/bin/env bash
# Builds dbt at a pinned commit, patched to use this repository's BigQuery driver. See README.md.
set -euo pipefail

# dbt's main branch, with Cargo.lock beside this script because dbt only commits one on release
# tags. Move them together.
readonly DBT_COMMIT=327f5269737bfec2440f10e07702f8280dfec1e9
readonly DBT_REPOSITORY=https://github.com/dbt-labs/dbt.git
readonly RELEASES=https://github.com/fornwall/bigquery-adbc/releases/download

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
work=$root/target/dbt-build
output=$root/dist
driver_archive=
driver_url=

usage() {
  echo "usage: $0 [--driver-archive PATH] [--driver-url URL] [--output-dir DIR] [--work-dir DIR]" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  [ $# -ge 2 ] || usage
  case $1 in
    --driver-archive) driver_archive=$(cd "$(dirname "$2")" && pwd)/$(basename "$2") ;;
    --driver-url) driver_url=$2 ;;
    --output-dir) output=$2 ;;
    --work-dir) work=$2 ;;
    *) usage ;;
  esac
  shift 2
done

case $(uname -s)-$(uname -m) in
  Linux-x86_64) platform=linux-x86_64 library_suffix=so ;;
  Linux-aarch64) platform=linux-aarch64 library_suffix=so ;;
  Darwin-x86_64) platform=macos-x86_64 library_suffix=dylib ;;
  Darwin-arm64) platform=macos-aarch64 library_suffix=dylib ;;
  *)
    echo "error: unsupported platform $(uname -s)-$(uname -m)" >&2
    exit 1
    ;;
esac

mkdir -p "$work" "$output"
work=$(cd "$work" && pwd)
output=$(cd "$output" && pwd)

# A release tag gives exactly its version, such as 0.3.0. Any other commit gives a name that
# identifies it, such as 0.3.0-2-g5f7f5be, and a modified checkout adds -dirty.
driver_version=$(git -C "$root" describe --tags --dirty --match 'v[0-9]*.[0-9]*.[0-9]*')
driver_version=${driver_version#v}

if [ -z "$driver_url" ] && [[ $driver_version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  asset=libadbc_driver_bigquery-$driver_version-$platform.$library_suffix.zst
  driver_url=$RELEASES/v$driver_version/$asset
fi

name=dbt-bigquery-adbc-$driver_version-$platform
stage=$work/stage/$name
rm -rf "$work/stage"
mkdir -p "$stage/licenses/dbt/patches" "$stage/licenses/bigquery-adbc"
cp "$root"/python/LICENSE "$root"/python/NOTICE "$root"/python/licenses/* \
  "$stage/licenses/bigquery-adbc/"

# The variables are compile-time switches, so a value inherited from the caller would change the
# build. A release build downloads its library, so it hashes the published bytes; anything else
# carries its library in lib/dbt/, where dbt looks for installed drivers.
unset BIGQUERY_ADBC_DRIVER_URL BIGQUERY_ADBC_DRIVER_VERSION BIGQUERY_ADBC_DRIVER_SHA256
if [ -n "$driver_url" ]; then
  curl --proto '=https,http' --fail --silent --show-error --location "$driver_url" \
    --output "$work/driver.zst"
  if command -v sha256sum > /dev/null; then
    driver_sha256=$(sha256sum "$work/driver.zst")
  else
    driver_sha256=$(shasum -a 256 "$work/driver.zst")
  fi
  export BIGQUERY_ADBC_DRIVER_URL=$driver_url
  export BIGQUERY_ADBC_DRIVER_VERSION=$driver_version
  export BIGQUERY_ADBC_DRIVER_SHA256=${driver_sha256%% *}
else
  if [ -z "$driver_archive" ]; then
    driver_archive=$(
      cd "$root"
      uv run --project python --locked python tools/package_native.py \
        --output-dir "$work/driver" | tail -n 1
    )
  fi
  mkdir -p "$stage/lib/dbt"
  tar -xzf "$driver_archive" -C "$stage/lib/dbt" --strip-components=1 \
    "$(basename "$driver_archive" .tar.gz)/libadbc_driver_bigquery.$library_suffix"
fi

source_dir=$work/dbt
patches=("$root"/tools/dbt-build/patches/*.patch)
git init --quiet "$source_dir"
# Reapplying would touch the patched files and rebuild most of the dbt workspace.
if [ "$(git -C "$source_dir" rev-parse --verify --quiet HEAD)" != "$DBT_COMMIT" ] ||
  ! git -C "$source_dir" apply --reverse --check "${patches[@]}" 2> /dev/null; then
  git -C "$source_dir" fetch --quiet --depth=1 "$DBT_REPOSITORY" "$DBT_COMMIT"
  git -C "$source_dir" checkout --quiet --force --detach FETCH_HEAD
  git -C "$source_dir" clean --quiet --force -d
  git -C "$source_dir" apply "${patches[@]}"
fi
cp "$root/tools/dbt-build/Cargo.lock" "$source_dir/Cargo.lock"

dbt_date=$(git -C "$source_dir" show --no-patch --format=%cs HEAD)
identifier="(modified with bigquery-adbc $driver_version) ${DBT_COMMIT:0:10} $dbt_date"

# dbt pins its own toolchain, which rustup selects from the checkout it runs in.
(
  cd "$source_dir"
  rustup toolchain install
  BIGQUERY_ADBC_DBT_IDENTIFIER=$identifier \
    cargo build --release --locked --bin dbt --target-dir "$work/target"
)

cp "$work/target/release/dbt" "$stage/dbt"
strip "$stage/dbt"
if [ "$(uname -s)" = Darwin ]; then
  # Stripping invalidates the linker's ad-hoc signature, and arm64 macOS kills unsigned binaries.
  codesign --force --sign - "$stage/dbt"
fi
cp "$source_dir/LICENSE" "$stage/licenses/dbt/"
cp "${patches[@]}" "$stage/licenses/dbt/patches/"

reported=$("$stage/dbt" --version)
if [ "$reported" != "dbt $identifier" ]; then
  echo "error: dbt --version reported '$reported', expected 'dbt $identifier'" >&2
  exit 1
fi

tar -czf "$output/$name.tar.gz" -C "$work/stage" "$name"
echo "$output/$name.tar.gz"
