# Release binary size

The driver is a `cdylib` loaded into another process, so file size and mapped code are release
constraints. This file is for contributors changing dependencies or the build profile. It records
the size of the stripped release library, how to reproduce it, and the measured choices that keep
it small. Judge a change by whole-artifact bytes: several changes here cut `.text` and still grew
the file.

Each measurement describes the tree it was made on, not necessarily the current checkout. Use the
commands below to measure the library you intend to distribute.

## Usage

Building requires Python 3 to parse Cargo's artifact report (`BIGQUERY_ADBC_PYTHON` overrides
`python3`). Inspection uses `size`, `strip` and `nm`, preferring their LLVM versions when present.

```sh
mkdir -p target
scripts/report-binary-size.sh > target/release-size.tsv          # build, then report
scripts/report-binary-size.sh --artifact target/release/libadbc_driver_bigquery.so --profile release

# Crate and symbol attribution needs an unstripped build of the same tree.
CARGO_PROFILE_RELEASE_STRIP=none CARGO_TARGET_DIR=target/size-symbols \
  cargo rustc --locked --release --lib --crate-type cdylib
scripts/report-binary-size.sh \
  --artifact target/release/libadbc_driver_bigquery.so \
  --symbols-from target/size-symbols/release/libadbc_driver_bigquery.so
```

The tab-separated report identifies the artifact by path, format and SHA-256, and records its
size, per-section byte counts and top `crate` and `symbol` rows (`--top COUNT`, default 20).
Named text differs from `.text` because of inlining and linker-generated code; use it to find
candidates, then compare whole-artifact bytes.

In report version 2, `context` rows describe the checkout and tools running the report. They do
not establish a supplied artifact's source revision, compiler, profile or target. Keep those
build records alongside saved libraries. The report records `CARGO_BUILD_TARGET` when set;
Cargo configuration can also select a target, so an unset variable does not imply a host build.

Every delta below is a paired build of two copies of one tree
(`git archive HEAD | tar -x -C <dir>`) that differ only in the change, using
`cargo rustc --locked --release --lib --crate-type cdylib` on `x86_64-unknown-linux-gnu` with
default features and the checked-in profile. A change to only paths or `-C metadata` moves the
artifact by a few hundred bytes (328 measured), so smaller deltas are noise. Each delta was
measured against the tree of its time; deltas from before the library was built alone as a
`cdylib` were measured without LTO.

Cargo applies the profile's `lto` only to a target whose crate types all support it, which an
`rlib` does not. The manifest therefore declares only the `rlib` that tests, benchmarks and fuzzing
use, and every shipped and tested build of the shared library runs
`cargo rustc --crate-type cdylib`.

## Baseline

| Metric | Value |
| --- | ---: |
| Rust/Cargo | 1.99.1 |
| Artifact bytes | 8,858,024 |
| `.text` | 6,467,014 |
| `.eh_frame` | 761,412 |
| `.rodata` | 762,533 |
| `.gcc_except_table` | 345,060 |
| `.rela.dyn` | 237,792 |
| `.data.rel.ro` | 158,952 |
| `.eh_frame_hdr` | 105,564 |
| `.got` | 1,816 |

The shipped x86-64 Linux wheel is smaller by the
[identical code folding (ICF) and path remapping](#the-linux-wheel-folds-code-and-remaps-paths) its
build adds.

## Release builds compile logging out

`tracing` and `log` are built with `release_max_level_off`. Nothing in the library installs a
subscriber or logger, and its copies of both crates are private to the shared library, so every
dependency event went to a no-op dispatcher. The size is per callsite, in `h2`, `hyper`, `tonic`,
`reqwest`, `rustls` and the transports of `google-cloud-gax` (gax):

| `tracing` cap | `log` cap | Artifact | Delta |
| --- | --- | ---: | ---: |
| `debug` | `debug` | 9,814,880 | |
| `warn` | `warn` | 9,637,696 | -177,184 |
| `off` | `debug` | 9,625,888 | -188,992 |
| `off` | `off` | 9,592,896 | -221,984 |

Both caps are needed because `rustls` logs through `log`, so `log` and `tracing` are dependencies
the driver never calls. Tests and debug builds keep every level; what the caps mean for a Rust
application that depends on this crate is in the [Rust API reference](REFERENCE.md#rust-api). The
driver's own diagnostics are `src/diag.rs`, a stderr writer enabled by
`ADBC_DRIVER_BIGQUERY_LOG_LEVEL` that costs 5,328 bytes; a `tracing` subscriber would need at
least the `warn` cap, 44,800 bytes more.

## Decisions that keep it small

| Decision | Bytes |
| --- | ---: |
| The library builds alone as a `cdylib`, so fat LTO applies; a manifest `cdylib` beside the `rlib` skips LTO | -1,123,216 |
| REST through `src/rest/` with driver-owned `serde` structs, not the generated `dyn` service clients that link every RPC and model: metadata, query path, model types | -874,304; -2,370,176; -212,944 |
| One JSON decoder per type: nested documents decode from `&RawValue` with `from_slice`, never `from_value` | -113,176 |
| `&str` input decodes through `from_slice(s.as_bytes())`, not `from_str`, at five sites | -7,616 |
| Exception: the REST row array uses `from_str` after one UTF-8 check, for a 15–19% faster row decode (`benches/rest_rows.rs`) | +42,560 |
| The row page's schema, job reference and errors decode with `from_slice` | -35,432 |
| `src/arrow_ipc_bigquery` replaces `arrow-ipc`, which stays a test oracle | -1,323,880 |
| LZ4 blocks decode in place, without `lz4_flex`'s frame decoder | -6,080 |
| Gathers use type-erased `MutableArrayData`; one generic `arrow_select::concat` import instantiated 780,091 bytes | |
| Shared `#[cold] #[inline(never)]` error and FFI panic paths | -109,984 |
| Module-local error constructors cold; `BigQueryType::from_sql` out of line | -12,352; -8,200 |
| `DriverRuntime::block_on` erases the future type; stack pinning removes the original box | -129,976 for boxed erasure; +1,088 to remove the allocation |
| `ffi::handle::dispatch` takes `&mut dyn FnMut`; metadata column builders take `&mut dyn Iterator`; three metadata paths lose a type parameter | -86,120 |
| Cancellation retry and Dataproc submission take `dyn` closures | -4,712; -2,144 |
| Spawned futures boxed as task types Tokio already instantiates (`DetachedTask`, `ProducerTask`) | -15,328; -12,640; -8,064 |
| Boxing that shares no existing instantiation grows the file: `tables.get` fan-out task, `Operation::wait` | +1,504; +5,856 |
| Per-package `opt-level`: control-plane crates at `"z"`, the HTTP and TLS stream glue at `"s"`, data-path crates at 3. Versus no per-package levels; versus the same crates at 14 `"z"` and 56 `"s"` | -1,177,480; -67,168 |
| No overflow checks in dependencies except the four `arrow-*` crates whose arithmetic feeds `unsafe` indexing | -105,256 |
| Overflow checks kept in this crate, so the FFI guards report an ADBC error | +8,304 |
| HTTP/2 for REST, kept for multiplexing and safe replay after `REFUSED_STREAM` and `GOAWAY` | +170,288 |
| gRPC `google.rpc.Status` details as ProtoJSON error details ([REFERENCE.md](REFERENCE.md#errors)), through gax's `StatusDetails` serializer | +31,056 |
| Small `BTreeMap`s filled by `insert`, since `collect` instantiates driftsort per type | -13,648 |
| View authorizations in sorted vectors, not B-trees | -11,584 |
| Query parameter values never decoded | -17,280 |
| Option-enum alternatives use one formatter over static spellings, without per-enum collection and join code | -2,440 |
| Job reads and cancellation share one async retry state machine through a borrowed request factory | -9,848 |
| Dataproc submissions and Vertex template reads and submissions share their async retry state machine | -4,400 |
| REST transport and JSON diagnostics stay out of line on cold paths | -2,048 |
| Capped service-error summaries share one loop through a borrowed iterator | -1,600 |
| Canonical option-enum names share one case-insensitive scan | -1,952 |

The data-path crates at `opt-level` 3 are this crate, `arrow-*`, `serde_json`, `tokio`, `ring`,
`lz4_flex`, `chrono`, `once_cell`, `prost` and `num-bigint`. The stream glue kept at `"s"` is
`tokio-rustls`, `tonic-prost`, `http-body`, `http-body-util`, `httparse`, `slab` and `smallvec`;
moving it to `"z"` too would save 9,216 bytes. Measure a new entry with a paired build. A
root-package override (`[profile.release.package.bigquery-adbc]`) silently disables `strip = true`
and ships a 15 MB library; the `"*"` spelling does not.

### Shared option-name matching

Canonical option-enum names use one out-of-line scan over their declaration-order spelling
tables; an unmatched name takes the legacy-alias lookup. This saves 1,952 bytes (0.0225%), while
keeping the per-enum lookup and marking it `inline(never)` instead grew the artifact by 448 bytes.

The two larger key enums contain 81 variants together. A native benchmark of their public
lookup methods checked the speed tradeoff, alongside a two-value enum. Each reported range
contains two runs' median of five 3,000,000-call samples; runs alternated before/after/after/before
on an AMD Ryzen 9 7950X3D pinned to CPU 8. These measure local option parsing, not query latency.
Canonical large-key lookups improve, while tiny enums, aliases and misses take about 5–12 ns longer:

| Lookup | Before, ns/call | Shared scan, ns/call |
| --- | ---: | ---: |
| Priority value `INTERACTIVE` | 3.73–3.77 | 9.25–9.45 |
| Database project, canonical | 36.88–37.13 | 9.93–10.57 |
| Database project, legacy alias | 34.76–35.16 | 43.57–44.30 |
| Statement priority, canonical | 32.50–33.00 | 20.87–20.96 |
| Statement priority, legacy alias | 35.50–35.78 | 46.49–47.68 |
| Unknown statement key | 38.28–39.15 | 44.06–45.30 |

### The Linux wheel folds code and remaps paths

The `[tool.cibuildwheel.linux]` `environment` in `python/pyproject.toml` adds `-Wl,--icf=all`
(-54,144) and `--remap-path-prefix` for the container's registry and git-checkout roots (-37,376,
measured with a length-matched `CARGO_HOME`). They are set there because an environment
`RUSTFLAGS` replaces config `rustflags`, and only the container pins the registry path. `--icf=all`
folds only `.text`, so vtables and `Waker::will_wake` are unaffected. The one function-address
comparison, `fn_addr_eq` in `src/ffi/stream.rs`, stays safe while no other function is handed out
as a stream release callback. These figures are for x86-64, which links with `rust-lld`. The AArch64
wheel links with GNU ld, which has no `--icf`, so an override keeps only the remapping.

## Rejected options

| Option | Bytes | Reason |
| --- | ---: | --- |
| `-Wl,-z,pack-relative-relocs` | -622,488 | Requires glibc 2.36 or later; PyPA's newest x86-64 manylinux image has 2.34. |
| `--icf=all` with `--ignore-data-address-equality` | -534,856 | Folds vtables, so `Waker::will_wake` can return a spurious `true`. |
| `-C force-unwind-tables=no` | -69,184 | Stack walkers in the host process lose those frames. |
| `ring` provider without ChaCha20-Poly1305 | -60,360 | Needs a driver-built `CryptoProvider` copying rustls's private algorithm table. |
| Whole-binary `opt-level = "s"` | -2,496,384 | Deoptimizes row conversion, JSON decoding, IPC and normalization. |
| `"*"` `opt-level` wildcard, hot crates pinned back | +11,504 | Larger than the explicit list. |
| `lto = "thin"` / no LTO | +907,384 / +1,123,216 | Keep `fat` with `codegen-units = 1`. |
| `opt-level = 2` for this crate | -49,120 | Fat LTO's one optimization pass runs at this crate's level, so it also deoptimizes the data-path crates. |
| LLVM machine outliner | +159,888 | Outlined functions add unwind entries and relocated calls. |
| `-Wl,--no-eh-frame-hdr` | -168,208 | A caught panic aborts the host. |
| Sharing the complete enum-value rejection, with the wrong-type formatter out of line | +928 | Grows the file. |
| Marking five error-conversion boundaries cold and out of line | +256 | Within path/metadata noise. |
| Sharing option-typo search over static name slices | +32 | Cuts `.text` by 1,472 bytes, but the static name-table relocations offset the saving. |
| Stack-pinning `Operation::wait`'s input behind a helper taking `Pin<&mut (dyn Future<Output = T> + Send)>` | +1,624 | The virtual polling boundary grows the file even without boxing, and the awaited futures must be `Send`. |
| Also erasing that output to `()` through a borrowed `Option`, as the synchronous runtime bridge does | +11,592 | As above, and their results must be `Send` too. |
| Sharing `named` and `failed_to` through one cold formatter taking a prefix, context and `&dyn Display` | -224 | Within path/metadata noise. |
| `or_list` as one cold loop over `&mut dyn ExactSizeIterator<Item = &str>`, without its intermediate vector and joined string | +512 | Grows the file. |
| The same `or_list` loop, generic over the input slice | +192 | Within path/metadata noise. |
| `panic = "abort"` | | Kills the host instead of returning an ADBC error. |

## Linked dependencies

The driver crate's `cargo machete` configuration ignores only `log` and `tracing`; every other
normal dependency is used. `google-cloud-bigquery-v2`, `-lro` and `-longrunning` link zero named
text and cost build time only.

### Dependency feature audit

The pinned dependency graph was checked with `cargo machete` and independent scratch-manifest
feature removals. Machete found no unused direct dependency. Each of these five changes left the
resolved Linux normal/build package and feature list identical:

| Removed direct request | Why the feature remains enabled |
| --- | --- |
| Rustls `std`, `tls12`, `logging` | `google-cloud-auth` requests all three; the transports also require TLS support. |
| Reqwest `rustls-no-provider` | `google-cloud-auth` requires it. |
| Tokio `rt-multi-thread`, `sync` | GAX and the transports enable them. |
| Tokio-util `io` | `h2` enables it. |
| GAX default features | `google-cloud-gax` defines no default feature set. |

Deleting a redundant declaration therefore does not exclude its implementation. Keep explicit
requirements for the APIs the driver uses, instead of depending on incidental feature unification.
Compare the graph before and after each isolated manifest edit with:

```sh
cargo tree --locked --target x86_64-unknown-linux-gnu -e normal,build \
  --prefix none --no-dedupe --format '{p}|{f}' | sort -u
```

Two other requests do change features. Removing Reqwest `system-proxy` removes only its and
Hyper-util's system-proxy flags on Linux; the system-settings lookup is compiled only for macOS
and Windows, where removing it would change proxy behavior. Removing Reqwest `http2` excludes
REST multiplexing but keeps `h2` and Hyper's HTTP/2 implementation for gRPC. None of the seven
variants removes any of the 210 Linux normal/build package instances. The proxy and HTTP/2
experiments needed offline lockfile pruning in their scratch trees; versions were not upgraded.

The larger remaining candidates need a narrower SDK feature boundary: telemetry dependencies
are mandatory under GAX's `_internal-common`, and the BigQuery crate cannot select only the
Storage RPCs the driver uses. See the [upstream measurements](#upstream-only-weights).
YAML support and REST HTTP/2 have measured costs, but removing either changes supported behavior.

### YAML and authentication dependencies

`serde_norway` parses the one YAML option, the Dataproc batch dbt-core 2.0 sends, for 95,976
bytes, measured by parsing that option with `serde_json` instead. `saphyr-parser` with a
driver-side builder into `serde_json::Value` was 1,800 bytes larger, and 3,120 at
`opt-level = "z"`, so `serde_norway` stays.

`google-cloud-auth` uses `jiff` only for two AWS SigV4 date strings
([UP-69](UPSTREAM.md#up-69-aws-credential-formatting-pulls-in-jiff)). `jiff` and `jiff-core`, both
at `opt-level = "z"`, cost 150,808 bytes against a `[patch.crates-io]` substitute implementing only
those formats, and `jiff-core` at `opt-level` 3 would add 42,560. The driver links the real crates
so that license, SBOM and advisory tooling see what ships.

The pinned `google-cloud-auth` includes
[#7253](https://github.com/googleapis/google-cloud-rust/pull/7253), merged as `238d92cf`.
It boxes external-account subject-token sources so token providers, retry wrappers and cache
refresh tasks share one implementation across source types.

## Upstream-only weights

These need upstream changes. Each is measured by `[patch]`ing every `google-cloud-*` package to a
scratch copy of the pinned checkout, against a path-patched control with no edits.

| Weight | Bytes | Why it stays |
| --- | ---: | --- |
| `google_cloud_gax_internal::observability` | at least -150,648 | Instantiated per RPC future and transport layer; `_internal-common` always enables it. |
| Storage RPCs the driver never calls | -198,768 | The generated clients are `pub(crate)` `dyn` stubs; see [UP-56](UPSTREAM.md#up-56-storage-writes-generated-client-links-the-rpcs-its-own-writer-never-calls). |
| `tonic`'s load-balanced `Channel` | 354,060 named text | gax owns channel construction and requires tonic's `channel` feature. |
| `google-cloud-wkt`'s `from_object<T>` collects each status-detail type's map with `BTreeMap::from_iter`, a stable sort per type | -52,048 | A non-generic map copy without `@type` builds the same map; needs the change in `google-cloud-wkt`. |
