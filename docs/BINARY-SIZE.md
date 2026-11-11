# Release binary size

Measurements of the stripped release library and its dependency and profile choices. Compare
whole-file bytes: a smaller `.text` can still mean a larger file. Figures describe historical
trees; measure the checkout you intend to distribute.

## Usage

`scripts/report-binary-size.sh` needs Python 3 (`BIGQUERY_ADBC_PYTHON` overrides `python3`) and
uses `size`, `strip` and `nm`, preferring the LLVM versions.

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

The TSV records path, format, SHA-256, total and section bytes, and the top crates and symbols
(`--top COUNT`, default 20); inlining and linker-generated code make named text differ from
`.text`. `context` rows describe the reporting environment, not a supplied artifact's provenance,
so keep build records with saved libraries. `CARGO_BUILD_TARGET` is recorded when set, but Cargo
configuration can also select a target.

Deltas below compare one isolated change in two copies of a tree (`git archive HEAD | tar -x -C
<dir>`), built with `cargo rustc --locked --release --lib --crate-type cdylib` for
`x86_64-unknown-linux-gnu`, default features and the checked-in profile. Paths or `-C metadata`
alone moved the file 328 bytes; smaller deltas are noise. For a paired measurement on each tree:

```sh
CARGO_PROFILE_RELEASE_STRIP=none CARGO_TARGET_DIR=target/size-review \
  cargo rustc --locked --release --lib --crate-type cdylib
llvm-strip --strip-all -o target/size-review/driver-stripped.so \
  target/size-review/release/libadbc_driver_bigquery.so
stat -c '%s' target/size-review/driver-stripped.so
llvm-size -A target/size-review/driver-stripped.so
```

To compare with the [Go driver](GO-DRIVER-COMPARISON.md), build it stripped for the same Linux
target from its `go/` directory, recording its revision and Go version:

```sh
git rev-parse HEAD
go version
go build -tags driverlib -buildmode=c-shared -ldflags="-s -w" -o libadbc_driver_bigquery.so ./pkg
stat -c '%s' libadbc_driver_bigquery.so
size -A libadbc_driver_bigquery.so
```

## Baseline

| Metric | Value |
| --- | ---: |
| Rust/Cargo | 1.99.1 |
| Artifact bytes | 8,557,544 |
| `.text` | 6,323,910 |
| `.eh_frame` | 741,948 |
| `.rodata` | 647,845 |
| `.gcc_except_table` | 337,388 |
| `.rela.dyn` | 231,072 |
| `.data.rel.ro` | 155,256 |
| `.eh_frame_hdr` | 102,428 |
| `.got` | 1,816 |

The x86-64 Linux wheel is smaller by its
[code folding and path remapping](#the-linux-wheel-folds-code-and-remaps-paths).

## Release builds compile logging out

The library installs no logger or subscriber, so its private `tracing` and `log` copies use
`release_max_level_off`, removing callsites in `h2`, `hyper`, `tonic`, `reqwest`, `rustls` and
`google-cloud-gax` (gax):

| `tracing` cap | `log` cap | Artifact | Delta |
| --- | --- | ---: | ---: |
| `debug` | `debug` | 9,814,880 | |
| `warn` | `warn` | 9,637,696 | -177,184 |
| `off` | `debug` | 9,625,888 | -188,992 |
| `off` | `off` | 9,592,896 | -221,984 |

Both caps are needed because `rustls` uses `log`; debug builds keep all levels. For Rust
applications' feature unification, see [Rust API](REFERENCE.md#rust-api). The driver's own stderr
diagnostics (`src/diag.rs`) cost 5,328 bytes; a `tracing` subscriber would need at least `warn`,
adding 44,800.

## Decisions that keep it small

| Decision | Bytes |
| --- | ---: |
| The library builds alone as a `cdylib`, so fat LTO applies; a manifest `cdylib` beside the `rlib` skips LTO (Cargo applies it only when every crate type supports it) | -1,123,216 |
| REST through `src/rest/` with driver-owned `serde` structs, not the generated `dyn` service clients that link every RPC and model: metadata, query path, model types | -874,304; -2,370,176; -212,944 |
| One JSON decoder per type: nested documents decode from `&RawValue` with `from_slice`, never `from_value` | -113,176 |
| `&str` input decodes through `from_slice(s.as_bytes())`, not `from_str`, at five sites | -7,616 |
| Exception: the REST row array uses `from_str` after one UTF-8 check, for a 15–19% faster row decode (`benches/rest_rows.rs`) | +42,560 |
| The row page's schema, job reference and errors decode with `from_slice` | -35,432 |
| `idna_adapter` pinned to 1.0.0, `idna`'s stub back end, so `url` links no ICU4X UTS 46 tables and URL hosts must be ASCII; 21 packages leave the build | -130,904 |
| IAM's impersonation refusal decodes into three driver-owned structs, not gax's `rpc::Status`, whose typed detail decoder links every `google.rpc` detail type | -107,024 |
| `src/arrow_ipc_bigquery` replaces `arrow-ipc`, which stays a test oracle | -1,323,880 |
| LZ4 blocks decode in place, without `lz4_flex`'s frame decoder | -6,080 |
| Gathers use type-erased `MutableArrayData`; one generic `arrow_select::concat` import instantiated 780,091 bytes | |
| Shared `#[cold] #[inline(never)]` error and FFI panic paths | -109,984 |
| Module-local error constructors cold; `BigQueryType::from_sql` out of line | -12,352; -8,200 |
| `DriverRuntime::block_on` erases the future type; stack pinning removes the original box | -129,976 for boxed erasure; +1,088 to remove the allocation |
| `ffi::handle::dispatch` takes `&mut dyn FnMut`; metadata column builders take `&mut dyn Iterator`; three metadata paths lose a type parameter | -86,120 |
| Cancellation retry and Dataproc submission take `dyn` closures | -4,712; -2,144 |
| Spawned futures boxed as task types Tokio already instantiates (`DetachedTask`, `ProducerTask`) | -15,328; -12,640; -8,064 |
| Session creation, transaction control and connection cleanup share one `Box<dyn FnOnce() + Send>` blocking-task type (`DriverRuntime::spawn_detached_blocking`; 8,859,344 → 8,842,464, `.text` -11,392, `.eh_frame` -1,144); query submission and row conversion keep their own | -16,880 |
| Boxing that shares no existing instantiation grows the file: `tables.get` fan-out task, `Operation::wait` | +1,504; +5,856 |
| Per-package `opt-level`: control-plane crates at `"z"`, the HTTP and TLS stream glue at `"s"`, data-path crates at 3. Versus no per-package levels; versus the same crates at 14 `"z"` and 56 `"s"` | -1,177,480; -67,168 |
| `flatbuffers`, which reads and writes only per-message IPC headers, at `"z"` ([throughput](#flatbuffers-at-z)) | -16,792 |
| `adbc_core` (ADBC schemas, option names, errors) and `tokio-util` (cancellation tokens, the gRPC request buffer's sender) at `"z"`; `prost-types` there grows the file | -11,560; +1,152 |
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
| Canonical option-enum names share one out-of-line case-insensitive scan; an unmatched name takes the legacy-alias lookup (keeping per-enum lookups `inline(never)` instead: +448) | -1,952 |

The data-path crates at `opt-level` 3 are this crate, `arrow-*`, `serde_json`, `tokio`, `ring`,
`lz4_flex`, `chrono`, `once_cell`, `prost` and `num-bigint`. The stream glue at `"s"` is
`tokio-rustls`, `tonic-prost`, `http-body`, `http-body-util`, `httparse`, `slab` and `smallvec`;
moving it to `"z"` would save 9,216 bytes. A root-package override
(`[profile.release.package.bigquery-adbc]`) silently disables `strip = true` and ships a 15 MB
library; the `"*"` spelling does not.

### `flatbuffers` at `"z"`

Alternating pinned runs of the `ipc` benchmark (`taskset -c 8`; base, `"z"`, `"z"`, base) kept
every case's best median within -3% and +6% of the base, inside the base runs' own spread; the
largest ratio is the 1-row scalar encode at 11 ns. `memchr`, which `serde_json` scans strings with,
and `hashbrown` would save 2,816 and 2,072 bytes at `"s"`, but stay at 3.

The shared option-name scan trades speed: median of five 3,000,000-call samples per run, two runs
alternating before/after/after/before, AMD Ryzen 9 7950X3D pinned to CPU 8 (local parsing, not
query latency). The two large key enums hold 81 variants together:

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
(-54,144) and `--remap-path-prefix` for the container's registry and checkout roots (-37,376,
measured with a length-matched `CARGO_HOME`). They live there because an environment `RUSTFLAGS`
replaces config `rustflags`, and only the container pins the registry path. `--icf=all` folds only
`.text`, so vtables and `Waker::will_wake` are unaffected; the one function-address comparison,
`fn_addr_eq` in `src/ffi/stream.rs`, stays safe while no other function is handed out as a stream
release callback. Figures are for x86-64 (`rust-lld`); the AArch64 wheel links with GNU ld, which
has no `--icf`, so it keeps only the remapping.

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
| Marking five error-conversion boundaries cold and out of line | +256 | Within noise. |
| Marking eight credential rejection formatters and three option refusal constructors cold and out of line | +1,000 | `.text` falls by 1,088 bytes, but unwind metadata grows. |
| Sharing option-typo search over static name slices | +32 | Cuts `.text` by 1,472 bytes, but the static name-table relocations offset the saving. |
| Stack-pinning `Operation::wait`'s input behind a helper taking `Pin<&mut (dyn Future<Output = T> + Send)>` | +1,624 | The virtual polling boundary grows the file even without boxing, and the awaited futures must be `Send`. |
| Also erasing that output to `()` through a borrowed `Option`, as the synchronous runtime bridge does | +11,592 | As above, and their results must be `Send` too. |
| Sharing `named` and `failed_to` through one cold formatter taking a prefix, context and `&dyn Display` | -224 | Within noise. |
| `or_list` as one cold loop over `&mut dyn ExactSizeIterator<Item = &str>`, without its intermediate vector and joined string | +512 | Grows the file. |
| The same `or_list` loop, generic over the input slice | +192 | Within noise. |
| Control-plane modules for size: nightly `#[optimize(size)]` on the 836 functions of `ffi`, `connection`, `dbt`, `driver`, `clients`, `config`, `statement`, `cancellation`, `error`, `transaction` and `options` | -135,112 | Stable Rust sets `opt-level` per crate, so this needs those modules in a crate of their own. |
| `panic = "abort"` | | Kills the host instead of returning an ADBC error. |

## Linked dependencies

`cargo machete` ignores only `log` and `tracing`; every other normal dependency is used.
`google-cloud-bigquery-v2`, `-lro` and `-longrunning` link zero named text and cost build time only.

### Dependency feature audit

Removing each of these direct feature requests in a scratch manifest left the Linux normal/build
package and feature lists identical:

| Removed direct request | Why the feature remains enabled |
| --- | --- |
| Rustls `std`, `tls12`, `logging` | `google-cloud-auth` requests all three; the transports also require TLS support. |
| Reqwest `rustls-no-provider` | `google-cloud-auth` requires it. |
| Tokio `rt-multi-thread`, `sync` | GAX and the transports enable them. |
| Tokio-util `io` | `h2` enables it. |
| GAX default features | `google-cloud-gax` defines no default feature set. |

Keep explicit features for APIs the driver uses. Compare an isolated manifest edit with:

```sh
cargo tree --locked --target x86_64-unknown-linux-gnu -e normal,build \
  --prefix none --no-dedupe --format '{p}|{f}' | sort -u
```

Removing Reqwest `system-proxy` drops only its and Hyper-util's system-proxy flags on Linux (the
lookup is compiled only for macOS and Windows, where removing it changes proxy behavior). Removing
Reqwest `http2` drops REST multiplexing but keeps `h2` and Hyper's HTTP/2 for gRPC. None of the
seven variants removes any of the 189 Linux normal/build package instances. Larger candidates need
narrower SDK features ([upstream-only weights](#upstream-only-weights)).

### YAML and authentication dependencies

`serde_norway` parses the one YAML option, the Dataproc batch dbt 2.0 sends, for 95,976 bytes
(measured by parsing that option with `serde_json` instead). `saphyr-parser` with a driver-side
builder into `serde_json::Value` was 1,800 bytes larger, 3,120 at `opt-level = "z"`.

`google-cloud-auth` uses `jiff` only for two AWS SigV4 date strings
([UP-69](UPSTREAM.md#up-69-aws-credential-formatting-pulls-in-jiff)). `jiff` and `jiff-core` at
`opt-level = "z"` cost 150,808 bytes against a `[patch.crates-io]` substitute implementing only
those formats; `jiff-core` at `opt-level` 3 would add 42,560. The driver links the real crates so
license, SBOM and advisory tooling see what ships.

## Upstream-only weights

Each is measured by `[patch]`ing every `google-cloud-*` package to a scratch copy of the pinned
checkout, against a path-patched control with no edits.

| Weight | Bytes | Why it stays |
| --- | ---: | --- |
| `google_cloud_gax_internal::observability` | at least -150,648 | Instantiated per RPC future and transport layer; `_internal-common` always enables it. |
| Storage RPCs the driver never calls | -198,768 | The generated clients are `pub(crate)` `dyn` stubs; see [UP-56](UPSTREAM.md#up-56-storage-writes-generated-client-links-the-rpcs-its-own-writer-never-calls). |
| `tonic`'s load-balanced `Channel` | 354,060 named text | gax owns channel construction and requires tonic's `channel` feature. |
| `google-cloud-wkt`'s `from_object<T>` collects each status-detail type's map with `BTreeMap::from_iter`, a stable sort per type | -52,048 | A non-generic map copy without `@type` builds the same map; needs the change in `google-cloud-wkt`. |
