# dbt with bigquery-adbc

Builds [dbt](https://github.com/dbt-labs/dbt) at a pinned commit of its `main` branch, patched so
that BigQuery uses this repository's driver instead of the one dbt downloads from its CDN. Other
adapters are unchanged. `dbt --version` identifies the build and the dbt commit it came from:

```text
dbt (modified with bigquery-adbc 0.4.0) 327f526973 2026-11-07
```

dbt's own version is unchanged everywhere else, including `dbt_version` in Jinja and artifacts.

## Builds

| | Release build | Non-release build |
| --- | --- | --- |
| Made from | A `vX.Y.Z` tag | Any other commit |
| Published as | Assets of that GitHub release | Artifacts of the workflow run on `main` |
| Driver version | `0.4.0` | `0.3.0-2-g5f7f5be`, with `-dirty` for a modified checkout |
| Driver | Downloaded on first use | `lib/dbt/` in the archive |

A release build needs only the `dbt` binary. The first BigQuery connection downloads the driver from
the same GitHub release into dbt's cache, and verifies it against a SHA-256 compiled into the
binary, so the download cannot be swapped. Non-release builds have no stable public URL to download
from, so keep `lib/dbt/` next to the binary; a symlink to `dbt` works, but a copy without the library
fails rather than falling back to dbt's driver. `DBT_ADBC_BIGQUERY_USE_FOUNDRY` has no effect here.

## Building

Needs [rustup](https://rustup.rs/), [uv](https://docs.astral.sh/uv/), `protoc` and a C toolchain.
Linux and macOS only.

```sh
tools/dbt-build/build.sh
```

This writes `dist/dbt-bigquery-adbc-<driver version>-<platform>.tar.gz`, working in
`target/dbt-build/`. A clean checkout of a release tag makes a release build, which needs that
release's driver asset to be published already, because the binary carries its hash.

| Option | Purpose |
| --- | --- |
| `--driver-archive PATH` | Take a non-release build's library from this `tools/package_native.py` archive instead of building one. |
| `--driver-url URL` | Make a release build that downloads from this URL, to try the flow before publishing. |
| `--output-dir DIR` | Where to write the archive. |
| `--work-dir DIR` | Where to fetch and build dbt. |

## Testing

The [dbt integration suite](../../integration/dbt/README.md) runs against these builds. To exercise
a release build's download before publishing one, serve the compressed library and point a build
at it. A loopback `http` URL needs no certificate, and the hash still applies:

```sh
uv run --no-project --python 3.14 --with "setuptools>=84.2.1" \
  python tools/package_native.py --compressed-library --output-dir target/dbt-build/serve
uv run --no-project --python 3.14 python -m http.server 8765 --bind 127.0.0.1 \
  --directory target/dbt-build/serve &
tools/dbt-build/build.sh --output-dir target/dbt-build/release --work-dir target/dbt-build/release \
  --driver-url "http://127.0.0.1:8765/$(basename target/dbt-build/serve/*.zst)"
uv run --directory integration/dbt --locked run.py \
  --dbt-archive "$PWD"/target/dbt-build/release/dbt-bigquery-adbc-*.tar.gz
```

Pass a published release's archive to `--dbt-archive` to test the real download.

## Updating dbt

dbt commits `Cargo.lock` only on release tags, so [`Cargo.lock`](Cargo.lock) here pins the
dependencies of the commit in `build.sh`. To move to a new commit, update `DBT_COMMIT`, refresh the
[patches](patches) until they apply, then regenerate the lockfile in the fetched checkout:

```sh
cd target/dbt-build/dbt
cargo update
cp Cargo.lock ../../../tools/dbt-build/Cargo.lock
```

Run the integration suite afterwards: dbt's `main` can change the options it sends to the driver.
