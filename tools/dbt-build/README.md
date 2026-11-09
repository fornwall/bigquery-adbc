# dbt with bigquery-adbc

Builds [dbt](https://github.com/dbt-labs/dbt) at a pinned commit of its `main` branch, patched so
that BigQuery uses this repository's driver instead of the one dbt downloads from its CDN; other
adapters are unchanged. The binary is named `dbt-bigquery-adbc`, so it can sit beside `dbt` on
`PATH`, and `--version` names the build and dbt commit:

```text
dbt (modified with bigquery-adbc 0.4.0) 327f526973 2026-11-07
```

dbt's own version is unchanged everywhere else, including `dbt_version` in Jinja and artifacts.

## Installing

On Linux and macOS, install the latest release into `~/.local/bin`:

```sh
curl -fsSL https://github.com/fornwall/bigquery-adbc/releases/latest/download/install.sh | sh
```

[`install.sh`](install.sh) is dbt's installer adapted to these releases, with the same options such
as `--version`, `--prefix` and `--update` (pass them after `sh -s --`). It checks the archive
against the release's `dbt-SHA256SUMS` and adds the directory to `PATH` in your shell's startup
file. The binary is not notarized, so macOS refuses a browser-downloaded copy until
`xattr -dr com.apple.quarantine <directory>`; a `curl` download avoids that.
`dbt-bigquery-adbc system update` reruns the latest release's `install.sh`. Neither supports
Windows; download its `.zip` from the release instead.

## Builds

| | Release build | Non-release build |
| --- | --- | --- |
| Made from | A `vX.Y.Z` tag | Any other commit |
| Published as | Assets of that GitHub release | Artifacts of the workflow run on `main` |
| Driver version | `0.4.0` | `0.3.0-2-g5f7f5be`, with `-dirty` for a modified checkout |
| Driver | Downloaded on first use | `lib/bigquery-adbc/` in the archive |

A release build's first BigQuery connection downloads the driver from the same GitHub release into
dbt's cache and verifies it against a SHA-256 compiled into the binary. A non-release build needs
`lib/bigquery-adbc/` next to the binary or under the prefix `install.sh` uses; a symlink to the
binary works, but a copy without the library fails rather than falling back to dbt's driver.
Neither reads stock dbt's `lib/dbt/` for BigQuery, so both can share a prefix such as `~/.local`
with a stock dbt without loading or replacing its driver.

### Switching to dbt's driver

`BIGQUERY_ADBC_USE_DBT_DRIVER=1`, read once per process, makes a run resolve BigQuery exactly as
stock dbt does, including its `lib/dbt/` and `DBT_ADBC_BIGQUERY_USE_FOUNDRY` (otherwise ignored):

```sh
dbt-bigquery-adbc build                                 # this driver
BIGQUERY_ADBC_USE_DBT_DRIVER=1 dbt-bigquery-adbc build  # dbt's driver
```

dbt caches the two drivers under different names in `~/.cache/com.getdbt/adbc/`, so once both are
downloaded, runs switch without downloading again. `dbt.log` reports `BigQuery driver resolved:`
followed by the driver used.

## Building

Needs [rustup](https://rustup.rs/), [uv](https://docs.astral.sh/uv/), `protoc` and a C toolchain;
on Windows, Git Bash with Visual Studio's C++ tools, and `--driver-archive` for a non-release build.

```sh
tools/dbt-build/build.sh
```

This works in `target/dbt-build/` and writes `dist/dbt-bigquery-adbc-<driver version>-<platform>.tar.gz`
(`.zip` on Windows). A clean checkout of a release tag makes a release build, which needs that
release's driver asset already published, because the binary carries its hash. Before packaging,
the script runs `dbt debug` with the [smoke profile](smoke/profiles.yml), whose fake key this driver
refuses with its own error, to check that dbt loads the driver; on a tag this checks the published
asset on every platform.

| Option | Purpose |
| --- | --- |
| `--driver-archive PATH` | Take a non-release build's library from this `tools/package_native.py` archive instead of building one. |
| `--driver-url URL` | Make a release build that downloads from this URL, to try the flow before publishing. |
| `--output-dir DIR` | Where to write the archive. |
| `--work-dir DIR` | Where to fetch and build dbt. |

## Testing

The [dbt integration suite](../../integration/dbt/README.md) runs against these builds. To exercise
a release build's download before publishing, serve the compressed library and point a build at it
(a loopback `http` URL needs no certificate, and the hash still applies):

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
dependencies of the commit in `build.sh`. To move commits, update `DBT_COMMIT`, refresh the
[patches](patches) until they apply, regenerate the lockfile in the fetched checkout, and rerun the
integration suite, since dbt's `main` can change the options it sends:

```sh
cd target/dbt-build/dbt
cargo update
cp Cargo.lock ../../../tools/dbt-build/Cargo.lock
```
