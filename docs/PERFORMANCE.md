# Performance

Measurements of the driver's hot paths and how to reproduce them, for contributors changing those
paths and evaluators comparing drivers. Microbenchmarks measure REST-to-Arrow decoding, ingest
normalization, and Storage API IPC encoding and decoding. A separate live benchmark measures
complete `dbt build` runs of [dbt-core 2.0](DBT-CORE.md). Binary size is in
[BINARY-SIZE.md](BINARY-SIZE.md).

The live-test project ID is anonymized as `example-project` here and in the saved measurement
artifacts. Use your own project when reproducing a run.

## Reproducing

```sh
cargo bench --locked --features benchmark --bench rest_rows   # REST result pages
cargo bench --locked --features benchmark --bench query_result # result scheduling and delivery
cargo bench --locked --features benchmark --bench normalize   # ingest normalization
cargo bench --locked --features benchmark --bench ipc         # Storage Read and Write IPC
uv run --directory integration/dbt --locked benchmark.py      # live dbt workload, needs BigQuery access
```

Each native harness prints, per case, the median of seven samples and the allocation calls and
requested bytes that the operation makes through the process allocator, including worker threads. Input
preparation, network I/O and server time are outside every measured region.

- `rest_rows` feeds one result page, as the JSON bytes BigQuery sends, at 1, 32 and 1,024 rows.
  `narrow` (2 columns) and `wide` (16) alternate INTEGER and STRING; `typed` has one column per
  decoder that does real work: TIMESTAMP, DATE, TIME, DATETIME, NUMERIC, BIGNUMERIC, BYTES,
  INTERVAL, a three-field STRUCT and a repeated INT64. `decode` is the `serde_json` pass into
  positional rows; `convert` is `rest_rows_to_record_batch` on a decoded page.
- `query_result` measures request serialization with 65-, 1,024- and 65,536-byte SQL targets
  (`query/request-*`, with the row-count column unused) and execution planning from stored SQL
  (`query/planning-*`). The fixtures include a default dataset, location and dbt invocation label;
  serialization measures an already-constructed plan. It also measures
  the production query response decoder and reader at 0, 1, 32 and 1,024
  rows, with the integer and two boolean columns of a dbt data-test result. `decode` includes
  operation tracking, the conversion permit and task scheduling; `read` starts from a decoded
  response and includes schema conversion, reader construction and draining to EOF; `complete`
  includes both. Its warmed runtime and anonymous client make no network requests. Unlike the
  decoder-only benchmarks, this harness measures work across runtime threads; avoid pinning it
  to a single CPU when measuring scheduling costs.
- `normalize` runs 2-column (`narrow`) and 16-column (`wide`) batches at 1 and 1,024 rows:
  `canonical` (value validation only), `calendar` (Date32 and TimestampMicrosecond), `nested` (the
  calendar columns under three struct levels), `list-window` (sliced DATE/TIMESTAMP lists, also
  at 65,536 rows), `coercing` (Int32 to Int64, LargeUtf8 to Utf8) and
  `list-gather` (sliced lists with values hidden behind NULL entries). `large-offsets` narrows
  sliced LargeUtf8 and LargeBinary arrays containing NULL, empty, ASCII and non-ASCII values,
  including an additional 65,536-row case. Dictionary fixtures cover ascending and scattered
  keys with sparse null runs, plus scattered keys alternating with nulls, at 1, 1,024 and 65,536
  rows.
- `ipc` decodes a Storage Read record-batch message (aligned, misaligned, and with `LZ4_FRAME`
  buffers as BigQuery sends them: one frame of independent 64 KiB blocks per buffer, with a stored
  block where LZ4 cannot shrink it) and encodes a Storage Write message. `scalar` has a nullable
  INT64 and a non-null STRING column; `nested` and `deep` wrap them in one and three levels of
  LIST<STRUCT>. Sliced encode cases retain nonzero offsets at 1,024 and 65,536 rows to model
  chunking large input batches.

Measuring a change:

- Compare before and after binaries in alternating runs. Pin CPU-only cases to one CPU
  (`taskset -c 3`); let `query_result` use multiple CPUs for its runtime workers.
- On a loaded machine, count instructions:
  `BENCH_COUNT=wide/convert/1024 perf stat -e instructions:u <binary>` runs that case 1,000 times.
  Process counts include fixture setup, input preparation and harness overhead. A `convert` count
  includes decoding its input, so subtract the matching `decode` count. Counts miss memory traffic,
  so confirm with pinned timings: converting REST rows a column at a time ran fewer instructions
  in `wide/convert/1024` and was still slower (see
  [rejected optimizations](#rejected-optimizations)).
- Check timings as well as allocations. Several changes below cut time with bit-identical
  allocation counts.
- The 1,024-row `coercing` cases sit in a ±12% code-alignment band: changing only an unrelated
  dependency's `opt-level` moved them about 12% with identical `.text` size. Compare a suspected
  regression there against a control build that changes only `-C metadata`.
- Run the binary Cargo just built. Its `target/release/deps/<bench>-<hash>` path changes whenever
  `Cargo.toml` does; take it from the `executable` field of
  `cargo bench --locked --features benchmark --no-run --message-format=json`.

## Results

Figures describe the code and workload measured, not necessarily the current checkout. A "before"
figure is the code preceding the change described, which this tree no longer contains, so the
[benchmark commands](#reproducing) reproduce only the "after" side, in the rows each section names.
Rerun both versions on the same machine when evaluating a change, and record the toolchain and CPU
with new results. Approaches that did not help are listed under
[rejected optimizations](#rejected-optimizations).

Native comparisons used Rust 1.99.1, locked dependencies and the release profile on Linux x86-64;
timed ones ran on an AMD Ryzen 9 7950X3D, pinned to CPU 3 unless a section says otherwise. They
alternated saved before and after executables. Each run reports the median of seven samples, and
tables give the median of three runs per executable. These results exclude network and BigQuery
work and establish no live dbt or upload speedup, and CPU affinity does not reserve a core against
other host activity. Requested bytes include intermediate allocations that have already been
freed; they are not retained memory or process RSS.

### Tiny query results

The `query_result` benchmark compared the earlier response path, inline response decoding, and
inline decoding with the resident reader, pinned to CPUs `0-7` so the runtime still had eight
workers. The table gives the median and range of three reports for a one-row result, in the
`query/complete` rows:

| Implementation | Complete local handling | Allocation calls | Requested allocation bytes |
| --- | ---: | ---: | ---: |
| Baseline | 25.92 µs (24.81–29.14) | 109 | 18,944 |
| Inline response decode | 18.68 µs (18.15–18.87) | 108 | 18,496 |
| Inline decode and resident reader | 3.43 µs (3.24–5.36) | 94 | 10,040 |

The synthetic response has dbt's `failures` integer and `should_warn`/`should_error` booleans,
plus query metadata. Its JSON is 335, 380, 1,807 and 47,441 bytes at 0, 1, 32 and 1,024 rows.
The one-row measurements include operation tracking, JSON decoding, schema conversion, reader
construction, Arrow conversion and draining to EOF. They exclude network I/O and BigQuery work.

Response JSON up to 4 KiB decodes inline while retaining the driver-wide conversion permit.
A complete resident page of at most 32 rows and 4 KiB converts lazily on its first read, using
the same permit and cancellation checks. This avoids a producer task and bounded delivery queue
for those results. Conversion errors still arrive during iteration; continuation tokens, wide
rows and larger results keep their existing paths. Conversion hands a host Tokio worker back
before running synchronously, since a tiny completion page can inherit a large earlier schema.

The 32-row complete path fell from a median 33.16 µs to 10.79 µs. The buffered 1,024-row control
ranged from 270–276 µs at baseline and 273–436 µs with the resident change, with a final sample of
273 µs, so these measurements establish no large-result throughput change.

Even the measured one-row saving totals only about 0.6 ms across the workload's 26 data tests,
before accounting for their concurrency. It is a reduction in local result-handling overhead;
the remote requests take hundreds of milliseconds, so this does not predict a visible dbt gain.

Three alternating before/after pairs then rebuilt existing relations with dbt `2.0.9`, at the
same eight threads, against `example-project` in `US`. Preparation stayed outside the timer.
All six timed builds verified the expected result signature, 13 CTAS jobs, 26 SELECT jobs and
zero cache hits.

| Library | Median dbt wall | Range | Median summed BigQuery service time |
| --- | ---: | ---: | ---: |
| Before | 8.86s | 8.51–11.00s | 17.11s |
| After | 8.61s | 8.19–8.70s | 15.86s |

The paired after/before median was `0.963`, ranging from `0.783` to `0.982`. This small sample,
changing BigQuery service time and host load cannot attribute the observed difference to the
driver change. The evidence supports lower local result-handling overhead, not an established
whole-build speedup.

Keep other CPU-heavy workloads idle when comparing scheduling costs.

#### Avoiding unused job clients

The REST job-client wrapper is built only when a result needs a metadata lookup or buffered
pagination, because resident and empty results already contain everything needed. Metadata
fetches and pagination share one wrapper per reader. These `query/complete` allocation counts
compare building it for every result with deferring it; concurrent compilation made timing
unsuitable for comparison:

| Rows | Allocation calls before → after | Requested bytes before → after |
| ---: | ---: | ---: |
| 0 | 73 → 70 | 7,160 → 6,962 |
| 1 | 94 → 91 | 10,040 → 9,842 |
| 32 | 128 → 125 | 14,608 → 14,410 |
| 1,024 (buffered control) | 1,140 → 1,140 | 174,512 → 174,512 |

#### Reusing the job client across statements

The REST job-client wrapper is cached with the database's immutable client configuration, so
submitting many small queries reuses its projects URL. The `query/client` row measures warmed
client acquisition, with no network requests:

| `query/client`, rows 0 | Allocation calls | Requested allocation bytes |
| --- | ---: | ---: |
| Before | 3 | 198 |
| Cached wrapper | 0 | 0 |

One wrapper and its URL remain resident per initialized configuration generation. Existing
connections retain their original configuration after a database option change; request-local
dataset-location resolvers stay outside the cache to avoid retaining that generation forever.
Mock endpoint and ownership tests cover both properties.

#### Stack-pinned runtime bridge

The synchronous runtime bridge pins its type-erased future on the calling thread's stack.
Type erasure still shares Tokio's blocking machinery across call sites, but the future needs no
heap allocation because it cannot outlive the synchronous call. This applies to query submission,
resident result conversion and other blocking calls, including calls from a dbt Tokio host.

Complete response handling in `query/complete`, before and after moving the erased future from the
heap to the stack:

| Rows | Allocation calls before → after | Requested allocation bytes before → after |
| ---: | ---: | ---: |
| 0 | 70 → 69 | 6,962 → 5,346 |
| 1 | 91 → 89 | 9,842 → 7,458 |
| 32 | 125 → 123 | 14,410 → 12,026 |
| 1,024 (buffered) | 1,135 → 1,133 | 174,322 → 172,674 |

Each response decode avoids one allocation. Nonempty resident results also avoid one for the
conversion permit; the buffered control avoids one when joining its producer at EOF. Consuming
an empty reader does not enter the runtime. The corresponding future state lives on the stack.
The isolated stripped-library cost was 1,088 bytes (0.013%); see [BINARY-SIZE.md](BINARY-SIZE.md).
The runtime tests cover nested multithreaded hosts, worker handoff and shutdown:

```sh
cargo test --locked --lib runtime::tests::
```

#### Shared statement SQL

Statements, execution plans and lazy parameter readers retain the same immutable SQL allocation.
Setting or replacing a statement's SQL still takes an owned snapshot; executing it no longer copies
that text into each plan or lazy reader. A mock regression replaces the statement text while its
parameter reader remains open, verifies that later bindings execute the original SQL, and checks
that the original allocation is released when the reader ends.

`query/planning-*` measures a fresh plan and request identity from already-stored SQL. A paired run
on Rust 1.99.1 / Ryzen 9 7950X3D, pinned to CPUs 8–15, reported:

| SQL target bytes | Allocation calls before → after | Requested bytes before → after |
| --- | ---: | ---: |
| 65 | 7 → 6 | 268 → 180 |
| 1,024 | 7 → 6 | 1,220 → 180 |
| 65,536 | 7 → 6 | 65,732 → 180 |

This removes one SQL-sized allocation per plan; the measured planning allocation is now independent
of SQL length. Input setup and request serialization are excluded. These are local allocation
measurements, not a live dbt latency claim. Reproduce with:

```sh
cargo bench --locked --features benchmark --bench query_result
cargo test --locked --lib statement::tests::execution::
```

#### Query request capacity

Reserving the SQL length plus 512 bytes for the usual request envelope avoids growing and
copying the buffer while serializing the SQL. This is a capacity hint: escaped SQL, parameters
and larger labels can still grow the buffer, and the request contents are unchanged. The
`query/request-*` rows cover serialization alone. Concurrent host activity made timings unsuitable
for a speed claim:

| SQL bytes | Allocation calls before → after | Requested allocation bytes before → after |
| ---: | ---: | ---: |
| 65 | 3 → 1 | 896 → 577 |
| 1,024 | 3 → 1 | 3,230 → 1,536 |
| 65,536 | 3 → 1 | 196,766 → 66,048 |

#### Uncontended resident conversion permits

A tiny resident result tries to acquire its conversion permit before entering the driver
runtime. If the permit is unavailable, the existing cancellable wait still applies. Conversion
continues to hand a Tokio host worker back before running, and retains the same driver-wide
concurrency limit. A closed semaphore still reports the existing one-shot pipeline error.

Three alternating before/after pairs of `query_result`, pinned to CPUs `8-15`, changed only the
resident permit acquisition:

| Case | Before ns | After ns | Allocation calls / bytes (unchanged) |
| --- | ---: | ---: | ---: |
| `query/read`, 1 row | 1,539 (1,532–1,583) | 1,432 (1,415–1,451) | 75 / 5,631 |
| `query/complete`, 1 row | 2,881 (2,858–2,964) | 2,778 (2,754–2,791) | 89 / 7,458 |
| `query/read`, 32 rows | 2,550 (2,514–2,569) | 2,418 (2,412–2,485) | 75 / 5,879 |
| `query/complete`, 32 rows | 9,654 (9,607–9,684) | 9,589 (9,422–9,716) | 123 / 12,026 |

The unchanged zero-row read control measured 1,297 → 1,313 ns, and the buffered 1,024-row control
55,736 → 57,470 ns; those paths do not use this fast path. A separate three-pair comparison
measured median process-wide instruction counts of 145,595,441 → 143,813,619 for 1,000
`query/read/1` calls, including input preparation and fixture startup.

Existing tests cover cancellation with an available permit and while waiting for a held permit;
the closed-pipeline regression checks error delivery and subsequent EOF.

#### Query schema conversion allocations

Schema conversion checks nesting depth during its existing field walk and borrows scalar type
names until inserting the Arrow metadata, avoiding a separate validation stack and a temporary
string for each scalar field. Tests cover a 256-level hostile input and siblings at the supported
depth limit. In `query/complete`, the three-column dbt data-test schema takes four fewer
allocations and requests 72 fewer bytes per result:

| Complete result | Allocation calls before / after | Requested bytes before / after |
| --- | ---: | ---: |
| 0 rows | 69 / 65 | 5,346 / 5,274 |
| 1 row | 89 / 85 | 7,458 / 7,386 |
| 32 rows | 123 / 119 | 12,026 / 11,954 |
| 1,024 rows | 1,133 / 1,129 | 172,674 / 172,602 |

Request serialization and response-decoding allocation counts are unchanged. Concurrent release
and fuzz builds affected timings, so no latency claim is made.

### Dataset location cache retention

The dataset-location cache holds at most 32 entries across a database's connections, evicting
the oldest inserted entry when full. Cache hits remain read-only hash lookups; revisiting an
evicted dataset repeats `datasets.get`. This bounds retained metadata for long-lived clients
that visit many scratch datasets without changing the location selected for a job.

A regression inserts 4,096 distinct dataset locations and retains the newest 32; an unbounded
cache retained all of them. Weak-reference assertions verify that eviction releases the dataset
name and location allocations. Separate tests cover replacement, invalidation, reinsertion and a
mock HTTP lookup after eviction. These measure retained entries and ownership, not process RSS or
live request latency.

```sh
cargo test --locked --lib clients::tests::
```

### Terminal reader retention

Result wrappers release their inner reader as soon as they observe EOF, an error or cancellation.
This frees queued result batches and unread bound inputs while keeping the wrapper's schema
available.

A regression uses a one-row INT64 slice backed by an 8,388,608-byte allocation. A wrapper that held
its inner reader after EOF or cancellation left two references (reader plus observer); the wrapper
leaves only the observer reference before `next()` returns. The tests also cover terminal errors,
cancellation arriving during the inner read, both wrapper variants, stable `schema()` and
subsequent EOF. An existing regression continues to check that an already-buffered metadata reader
remains readable after cancellation.

```sh
cargo test --locked --lib operation_readers_release_retained_buffers
cargo test --locked --lib cancellation::tests::
```

These are deterministic retained-buffer lifetime measurements, not process RSS or throughput
measurements. Caller-owned batches and allocations retained by the allocator are excluded.
Cancellation is observed when the caller next polls the wrapper; this does not make an unpolled
wrapper release its queued data immediately when a cancellation handle is invoked.

### Input buffer retention

A two-row Int64 slice backed by 8 MiB checks ownership. The driver releases its batch reference,
and with it all 8,388,608 backing-buffer bytes, before returning the final parameter row for
execution. The first row still retains the input because the second remains unread.
Empty sliced batches are also released before the next source callback. This measures a
deterministic buffer lifetime, not process RSS, and excludes buffers retained independently by
the caller or source reader. Reproduce with:

```sh
cargo test --locked --lib bound_parameter_batches
```

The Storage Write transport drops consumed batches, and writing or discarding an IPC plan releases
its buffer references while preserving scratch capacity, so a two-row slice cannot keep 8,388,608
bytes referenced during the next source read. The regression keeps one observer buffer and asserts
that no driver reference remains before reading the next batch, including the final EOF read. It
completes a 12-row mock ingest and commit; this measures buffer ownership, not process RSS or live
BigQuery performance:

```sh
cargo test --locked --lib the_first_ingest_batch_is_released_before_reading_more_input
cargo test --locked --lib written_and_discarded_plans_release_their_input_buffers
```

The normalized window is also released before waiting for an AppendRows response or stream
creation retry. A 1,048,576-row Float32 input normalizes to an 8,388,608-byte Float64 allocation.
Consuming the window during encoding reduces the driver's references to that allocation from
one to zero before the append starts, while the independently encoded IPC payload stays valid.
The regression verifies this on success and rejection, and round-trips every successful value:

```sh
cargo test --locked --lib encoded_chunks_release_their_normalized_window
```

This measures ownership of the normalization allocation, not peak RSS or live ingestion
throughput. The source reader's input buffers, encoded payload and allocator retention are
outside this measurement.

### Parameterized decimal buffer reuse

When Decimal128 or Decimal256 already has the target physical width and written scale, exact
declared precision/scale validation retains its value and null buffers. An unchanged Arrow type
also keeps the original array. A narrower declared scale still rejects nonzero discarded digits
and preserves row diagnostics.

`normalize` allocation counts before and after this reuse, for both the `decimal-precision` and
`decimal-scale` rows at 65,536 rows:

| Columns | Allocation calls before / after | Requested bytes before / after |
| --- | ---: | ---: |
| 2 | 7 / 1 | 3,146,096 / 32 |
| 16 | 49 / 1 | 25,168,768 / 256 |

Columns alternate Decimal128(38, 9) and Decimal256(76, 38), with one null per sixteen rows and
nonzero slice offsets. The remaining allocation holds the batch's column references; source buffers
are shared. Timings were unpaired under concurrent build and fuzz load, so these results establish
no throughput gain. Two alternating instruction-count pairs over 1,000 calls increased about
0.6–0.7%, including fixture preparation and drops. Tests check sliced buffers, null validity and
opaque null payloads, unchanged integer/fractional overflow rejection and row diagnostics.

### Nullable dictionary expansion

Bulk ingest gathers consecutive dictionary keys within each valid span and appends each null
span in one operation, where the presence of any null key would otherwise select a per-row gather,
including for long ascending runs. Key validation, null dictionary values, sliced validity
bitmaps and the existing zero-copy path for wholly consecutive non-null keys are preserved.

The `dictionary-*` rows of `normalize`, comparing the per-row gather with span gathering on the
same fixtures, at 65,536 rows:

| Dictionary keys | Columns | Before, ms | After, ms |
| --- | ---: | ---: | ---: |
| Ascending runs, four nulls per 32 rows | 2 | 2.787 | 1.474 |
| Ascending runs, four nulls per 32 rows | 16 | 14.601 | 4.572 |
| Scattered keys, four nulls per 32 rows | 2 | 1.688 | 1.474 |
| Scattered keys, four nulls per 32 rows | 16 | 14.408 | 12.887 |
| Scattered keys, alternating nulls | 2 | 1.280 | 1.347 |
| Scattered keys, alternating nulls | 16 | 10.246 | 10.806 |

Columns alternate INT64 and STRING dictionary values, with the input sliced by one row. Allocation
counts and requested bytes are unchanged: the two-column runs/scattered fixtures request 32
allocations and 4,932,496 bytes; the alternating-null fixture requests 31 and 2,835,344 bytes.
The wide ascending-run fixture is 3.2 times faster locally. Alternating nulls offer no runs to
combine and cost about 5–6% more in this fixture; this tradeoff is retained rather than adding a
null-density heuristic. Single-row valid-span and run-initialization special cases were also
measured and rejected because they weakened the other gains.

For the two-column cases, `perf stat` over 1,000 iterations counted 37.513→9.483 billion user
instructions for ascending runs, 37.512→32.850 billion for scattered keys, and 28.789→30.263
billion for alternating nulls. These counts include harness initialization, input preparation
and output drops.

Tests compare against Arrow's `take` kernel across nested value types, null runs, non-byte-aligned
slices and invalid live keys:

```sh
cargo test --locked --lib types::cast::tests::
```

### Large-offset byte columns

Narrowing LargeUtf8 and LargeBinary offsets validates their minimum and maximum before rebasing
them into one exactly sized vector per column, where fallible collection grew that vector
incrementally. STRING and BYTES payload buffers remain shared, and every offset must still fall
inside the validated span before narrowing. In the `large-offsets` rows of `normalize`, `narrow`
has two columns and `wide` has sixteen:

| Rows | Large-offset shape | Before µs | After µs | Allocation calls before → after | Requested bytes before → after |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,024 | narrow | 5.108 | 4.277 | 25 → 7 | 33,152 → 8,616 |
| 1,024 | wide | 41.475 | 34.231 | 193 → 49 | 265,216 → 68,928 |
| 65,536 | narrow | 418.285 | 275.058 | 37 → 7 | 2,097,536 → 524,712 |
| 65,536 | wide | 3,629.338 | 3,189.348 | 289 → 49 | 16,780,288 → 4,197,696 |

The 65,536-row narrow before/after ranges were 414.077–418.755 / 263.052–282.859 µs;
wide ranges were 3,577.868–3,651.120 / 3,178.662–3,210.377 µs. These cases use 75% fewer
requested allocation bytes and take 34.2% / 12.1% less local normalization time.

The existing 1,024-row `coercing/wide` fixture, alternating Int32 and LargeUtf8 columns, fell
from 20.303 to 17.411 µs, from 121 to 49 allocation calls, and from 199,616 to 101,472 requested
bytes. Canonical, calendar and list-gather wide controls kept their allocation counts and had
median times within 0.7%. Three paired instruction counts for 1,000 large-offset wide operations
fell from about 58.43 billion to 49.10 billion user instructions (16.0% fewer), including
identical fixture construction and input preparation.

The cast regression checks NULLs, empty values, non-ASCII strings, vector boundaries and
65,536-row slices, including that the original payload buffers remain shared:

```sh
cargo test --locked --lib types::cast::tests::
```

### Sliced Storage Write batches

Chunking a large input batch leaves nonzero STRING, BYTES and LIST offsets. The writer copies each
offset region once and rebases it in place, avoiding a vector extension and checked subtraction
for every offset. Planning still validates the offsets before writing them. The `encode-sliced`
rows of `ipc`, with the same fixtures in both executables and no other build or test running:

| Rows | Sliced shape | Before µs | After µs | Time reduction | Allocation calls / requested bytes, unchanged |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,024 | scalar | 1.496 | 1.143 | 23.6% | 3 / 22,024 |
| 1,024 | nested | 3.936 | 2.898 | 26.4% | 10 / 48,688 |
| 1,024 | deep | 16.244 | 11.419 | 29.7% | 33 / 205,512 |
| 65,536 | scalar | 80.125 | 59.492 | 25.8% | 3 / 1,384,840 |
| 65,536 | nested | 216.695 | 156.407 | 27.8% | 10 / 3,032,368 |
| 65,536 | deep | 985.749 | 688.309 | 30.2% | 33 / 12,914,376 |

The 65,536-row before/after ranges were 79.652–80.199 / 59.434–59.619 µs for scalar,
216.604–216.832 / 156.330–156.552 µs for nested, and 984.940–988.182 / 687.386–688.561 µs
for deep. The unsliced 1,024-row controls stayed within 1.5%: scalar 0.573→0.576 µs,
nested 1.111→1.113 µs and deep 3.558→3.507 µs, with unchanged allocations.

The largest deep fixture exceeds one AppendRows request budget and measures the codec
independently of the chunk encoder's existing request limits.

Large sliced BYTES/LIST coverage is checked against the independent `arrow-ipc` reader, including
null and empty entries and slice lengths around loop tails.

### Validating sliced list windows

Canonical sliced lists validate only the child values their offsets address. Validation keeps the
original offsets and translates error locations by the child window's origin; offsets are copied
and rebased only when normalization must rebuild the list. Nested lists and NULL-list rewrites
retain their row locations and values.

One paired run of `normalize`, pinned to CPU 3 with Rust 1.99.1 on a Ryzen 9 7950X3D, measured
these `list-window` cases. Each time is the harness median of seven samples; other builds were
running, so timings are indicative. Allocations are requested bytes, not peak RSS.

| Columns / rows | Before, ns | After, ns | Allocation calls before → after | Requested bytes before → after |
| --- | ---: | ---: | ---: | ---: |
| 2 / 1,024 | 2,168 | 886 | 6 → 2 | 8,536 → 224 |
| 16 / 1,024 | 17,065 | 6,909 | 48 → 16 | 68,288 → 1,792 |
| 2 / 65,536 | 133,329 | 47,117 | 6 → 2 | 524,632 → 224 |
| 16 / 65,536 | 1,001,822 | 370,984 | 48 → 16 | 4,197,056 → 1,792 |

The wide 65,536-row fixture avoids 4,195,264 requested bytes per normalization. The remaining
allocations describe child slices; this measures validation, excluding IPC, transport and BigQuery.

```sh
cargo bench --locked --features benchmark --bench normalize --no-run --message-format=json
# Run the reported executable, then compare its list-window rows with the preceding version.
taskset -c 3 <normalize-executable>
cargo test --locked --lib ingest::normalize::
```

### Offset validation for sliced Storage Write batches

The writer's offset validation reduces to the smallest offset, where a scan that could return at
every offset would keep the compiler from vectorizing the valid-input path. The minimum must still
be at least the nonnegative first offset, which proves every later rebase is nonnegative and
cannot overflow. Invalid inputs still fail before any body is materialized. Allocation calls and
requested bytes were unchanged from the preceding sliced-write table:

| Rows | Sliced shape | Before µs | After µs | Time reduction |
| ---: | --- | ---: | ---: | ---: |
| 1,024 | scalar | 1.201 | 0.821 | 31.6% |
| 1,024 | nested | 2.961 | 1.977 | 33.2% |
| 1,024 | deep | 11.667 | 6.643 | 43.1% |
| 65,536 | scalar | 60.883 | 41.123 | 32.5% |
| 65,536 | nested | 159.263 | 97.141 | 39.0% |
| 65,536 | deep | 703.112 | 392.241 | 44.2% |

The 65,536-row before/after ranges were 60.103–60.967 / 40.986–53.750 µs for scalar,
157.576–159.611 / 96.855–100.296 µs for nested and 693.673–706.400 / 391.180–404.297 µs
for deep; other host workloads added visible jitter. Unsliced 1,024-row control medians were
0.584→0.565, 1.135→1.118 and 3.651→3.551 µs; no unsliced speedup is claimed.

Instruction counts provide a less timing-sensitive check: three alternating runs of 1,000 deep
65,536-row sliced encodes fell from a median 11,892,573,353 to 4,141,286,435 user instructions
(65.2% fewer). These process counts also include identical fixture construction and validation.

Writer tests cover invalid offsets at every interior position around vector-width boundaries,
including `i32::MIN` and `i32::MAX`, and existing independent `arrow-ipc` round trips cover valid
sliced output.

### Live dbt workload

dbt `2.0.9` ran the default 13-model, 26-test workload (50,000 base rows, eight threads, fresh
datasets) on `example-project` in `US`. The host was Linux x86-64 on an AMD Ryzen 9 7950X3D; this
driver's release library used the pinned Rust `1.99.1` and locked dependencies. Three rounds
alternated dbt's opt-in [Foundry driver](DBT-CORE.md#installing) `0.21.0.dev+dbt0.1.5` and this
driver, before the metadata change below:

| Driver | Median wall | Range | Median jobs/s | Median warehouse span |
| --- | ---: | ---: | ---: | ---: |
| Downloaded Foundry | 23.92s | 23.39–28.62s | 1.63 | 20.01s |
| This driver, before the metadata change | 8.64s | 8.39–10.31s | 4.51 | 6.58s |

The paired local/Foundry wall-time ratio was `0.359` (64.1% lower). Every timed invocation ran
13 CTAS jobs and 26 SELECT jobs, with no cache hits and the expected warehouse aggregate
signature. Median summed BigQuery service time was 18.42s for Foundry and 16.18s locally. This
comparison uses the downloaded opt-in driver, distinct from the historical default-driver result
in [integration/dbt/PERFORMANCE.md](../integration/dbt/PERFORMANCE.md#historical-default-driver-result).

The metadata change makes `GetTableSchema` request `tables.get(view=BASIC)` with
the same schema and configuration fields. `FULL` also requests storage accounting, which dbt's
relation configuration does not consume. Google's [tables.get reference](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables/get#TableMetadataView)
documents that distinction. `GetStatistics` still requests `STORAGE_STATS`.

In 24 alternating BASIC/FULL request pairs per relation, using identical field selectors and
one warmed authenticated HTTP session, all projected responses were equal:

| Relation | FULL median | BASIC median | Median paired saving |
| --- | ---: | ---: | ---: |
| Partitioned, clustered table | 282.8ms | 200.4ms | 63.0ms |
| Logical view | 289.6ms | 181.3ms | 75.6ms |
| Materialized view | 254.7ms | 207.6ms | 52.9ms |

A second 24-pair run using the checked-in metadata benchmark also returned equal configurations.
Its paired savings were 57.6ms for the table, 40.1ms for the view and 35.8ms for the materialized
view.

These HTTP measurements isolate the metadata view; they do not measure the Rust transport or
establish an end-to-end dbt improvement. Relation lookups run concurrently, so their individual
savings do not add directly to process wall time. The live Rust test
`table_metadata_describes_what_defines_each_relation` verified configuration for tables, views,
materialized views and external tables after the change. Regression tests separately pin BASIC
for relation schemas and STORAGE_STATS for exact row counts.

Five before/after pairs rebuilt existing relations with `--existing`, alternating which library
ran first. Preparation ran outside the timer; each timed invocation again verified 39 jobs,
zero cache hits and the expected result signature:

| Library | Median wall | Range | Median summed BigQuery service time |
| --- | ---: | ---: | ---: |
| Before | 9.82s | 8.49–16.52s | 19.72s |
| After | 9.88s | 8.53–10.86s | 17.93s |

The paired after/before median was `0.931`, ranging from `0.546` to `1.181`, with three pairs
improving and two regressing. The similar wall medians and large spread do not establish an
end-to-end dbt speedup. The supported improvement is the lower cost of configuration lookups.

A final three-round comparison of the updated library against Foundry, both rebuilding existing
relations, gave:

| Driver | Median wall | Range | Median jobs/s | Median warehouse span |
| --- | ---: | ---: | ---: | ---: |
| Downloaded Foundry | 25.39s | 24.86–28.16s | 1.54 | 18.36s |
| This driver | 10.23s | 9.78–17.43s | 3.81 | 8.36s |

The paired local/Foundry ratio was `0.403` (59.7% lower wall time). All six timed builds verified
the same 39 jobs, zero cache hits and expected result signature; successful scratch datasets were
deleted. This is the comparison between drivers, not a measure of the metadata change alone.

Reproduce the current Foundry comparison and metadata measurement:

```sh
uv run --directory integration/dbt --locked benchmark.py
uv run --directory integration/dbt --locked benchmark.py --existing
uv run --directory integration/dbt --locked benchmark_metadata.py --include-materialized-view
```

The [benchmark guide](../integration/dbt/PERFORMANCE.md) describes workload controls, isolated
driver caches, verification, preserved artifacts and `--local-library` for comparing saved release
libraries. Query execution and network variance are substantial; timings have no CI threshold.

#### Repeated dataset discovery

The two dataset listings traced in the installed dbt `2.0.9` have different callers. Schema
registration calls `GetObjects` at schema depth without a schema filter. The first relation-cache
lookup then calls it at table depth with the target dataset name. Underscores in that name are
ADBC pattern wildcards, so the driver enumerates datasets again before filtering. A temporary
ADBC wrapper recorded sequential calls on the same connection and no connection-option setup
by this dbt binary; neither request was a retry.

Setting the existing `bigquery.get_objects.disable_wildcards=true` option removed the second
`datasets.list` in a live build over populated relations. Both diagnostic builds passed all
39 jobs, with no query-cache hits and the expected aggregate signature. An empty dataset still
needs `datasets.get` after an empty `tables.list` to distinguish it from a missing dataset, so
this does not guarantee fewer requests for a fresh dataset. These runs do not establish a
whole-build speedup. The existing mock test reproduces the direct table listing:

```sh
cargo test --locked --all-features disabling_wildcards_keeps_unfiltered_dataset_and_table_listings
```

[Upstream dbt commit `7f6844449`](https://github.com/dbt-labs/dbt/commit/7f6844449b13b920dc9ea4da49806d1d835f718b)
enables literal filters best-effort in `crates/dbt-adapter/src/engine/adbc.rs` on upstream `main`;
the `2.0.9` binary lacks it. This driver accepts the option. The
[canonical](DBT-CORE.md#installing) `0.21.0.dev+dbt0.21.19` and opt-in Foundry `0.21.0.dev+dbt0.1.5` libraries return
`NOT_IMPLEMENTED` for `disable_wildcards` and `skip_table_metadata`. Caller configuration and
driver support must both be present; a driver-wide dataset cache or changing default ADBC
wildcard semantics is unnecessary.

### Result pages and Storage Read

These measurements compare a row-count and row-width threshold that the
[continuation-token rule](#choosing-storage-read-for-every-paginated-result) replaced, to handle
wide rows on later pages.

Seconds to read a whole result in one Rust process against `example-project` with
`bigquery.query.disable_query_cache`, over two rounds per build, alternating builds. Narrow rows are
`SELECT n FROM UNNEST(GENERATE_ARRAY(1, N)) AS n ORDER BY n`; padded rows add
`REPEAT('x', 500) AS padding`, so a 10 MB page holds about 20,000 of them. "Before" sends
`jobs.query` without `maxResults` (a first response of up to 350,000 narrow rows), pages by 10,000
rows and uses Storage Read from three pages left; "after" used 100,000 rows per
response, and Storage Read from two pages or 80,000 rows left.

| Result | Before | After | REST only, before | REST only, after |
| --- | ---: | ---: | ---: | ---: |
| 1,000 narrow rows | 0.45–0.56 | 0.49–0.51 | | |
| 50,000 narrow rows | 0.97–1.25 | 1.10–1.21 | | |
| 100,000 narrow rows | 2.45–2.68 | 2.47–2.75 | | |
| 200,000 narrow rows | 4.70–5.16 | 3.99–5.52 | 4.49–5.92 | 4.63–4.68 |
| 600,000 narrow rows | 8.49–8.72 | 4.73–5.22 | 19.89–20.42 | 13.57–14.49 |
| 60,000 padded rows | 3.33–3.68 | 3.55–3.63 | 4.76–4.85 | 3.84–4.37 |

That threshold came from the same setup with Storage Read forced at any size against
`bigquery.query.use_storage_api_disabled_client=true`, at 100,000-row pages:

| Result | Pages after the first | Storage Read | REST |
| --- | ---: | ---: | ---: |
| 120,000 narrow rows | 1 (20,000 rows) | 4.35–4.97 | 3.25–3.29 |
| 160,000 narrow rows | 1 (60,000 rows) | 4.50–4.56 | 4.02–4.20 |
| 200,000 narrow rows | 1 (100,000 rows) | 4.08–4.12 | 4.72–4.80 |
| 40,000 padded rows | 1 | 3.24–3.47 | 3.03–3.12 |
| 50,000 padded rows | 2 | 3.45–3.71 | 3.88–4.09 |
| 6,000 rows of 3,000 bytes | 1 | 2.67–3.33 | 2.23–2.42 |
| 10,000 rows of 3,000 bytes | 2 | 3.08–4.61 | 2.89–2.99 |

In these runs, the `jobs.get` and read-session setup for Storage Read took about a second before
rereading the whole result. A REST page took a round trip plus about 20 µs per narrow row: for
600,000 narrow rows, the first `jobs.query` response took 8.4–8.5 s holding 350,000 of them and
3.1–3.2 s holding 100,000.

### Choosing Storage Read for every paginated result

The first completed response's continuation token selects Storage Read before any rows are
delivered. A first page's average row width cannot rule out a later row exceeding REST's row
limit. Complete first responses still use REST, and explicit disabling or an unavailable
Storage Read session retains REST fallback. Selecting by total row count is a
[rejected optimization](#rejected-optimizations).

Measured against `example-project` in `US`, Linux x86-64, AMD Ryzen 9 7950X3D, Rust 1.99.1,
`adbc_driver_manager` 1.12.0, and PyArrow 26.0.0. Both builds used the default features and the
release `cdylib` build. The baseline used a row-count/width threshold; the candidate replaced it
with the continuation-token rule. Each connection was warmed by one query, then five pairs ran in
one Python process with alternating build order and query cache disabled. Timings include query
execution, fetching every batch, and checking row counts, integer sums, and padding lengths. The
queries generate data without scanning tables.

| Result | Baseline, median (range), s | Continuation-token rule, median (range), s | Median change |
| --- | ---: | ---: | ---: |
| 1,000 narrow rows, complete first response | 0.437 (0.419–0.490) | 0.440 (0.386–0.500) | +0.003 s |
| 120,000 narrow rows, one further REST page | 3.574 (3.415–3.936) | 4.011 (3.694–4.585) | +0.437 s, +12.2% |
| 6,000 rows with 3,000-byte padding, one further REST page | 2.450 (2.351–2.547) | 2.981 (2.788–3.073) | +0.531 s, +21.7% |

Debug diagnostics confirmed all ten baseline paginated reads used REST and all ten candidate
reads used Storage Read. The complete-response control stayed on REST. These small paginated
results pay extra setup and rereading time for the guarantee that a later wide row will not
fail REST after an earlier page has already been delivered. The control's ranges overlap; these
network measurements establish no change for complete responses.

The live correctness regression uses 2,000 ordinary rows followed by one row containing thirty
800,000-byte strings. It checks all 2,001 row indices and every string, including the 24 MB final
row that cannot be returned by REST:

```sh
scripts/run-one-live-test.sh reads_a_later_row_too_large_for_rest_through_storage_read
```

## Retained result-buffer memory

The pipeline regression fixtures check the buffer capacity retained by small Arrow slices against
the completed-batch budget. Retained-capacity accounting counts each backing allocation once per
batch, including nested buffers and validity bitmaps; the previous charge used logical slice
lengths.

| Fixture | Retained buffer bytes | Previous charge | Retained-capacity charge |
| --- | ---: | ---: | ---: |
| Nine 16-row INT64 columns sharing one 8 MiB allocation | 8,388,608 | 1,152 | 8,388,608 |
| One INT64 row in an 8 KiB allocation | 8,192 | 8 | 8,192 |

These are deterministic allocation-capacity and semaphore measurements, not process RSS or
throughput measurements. A sliced external response buffer is also charged its full 8 MiB
extent. The backpressure regression holds the 8 KiB fixture in an 8 KiB budget, verifies that a
second batch cannot enter, then releases the first batch and verifies that the second proceeds.
The oversized-batch regression retains more than 64 MiB behind one row and verifies that the
existing whole-budget clamp still allows delivery without deadlock.

Reproduce the fixtures and budget checks:

```sh
cargo test --locked --lib pipeline::tests::
```

Sharing across separate batches is charged independently. This conservative accounting avoids
making one batch's permit depend on the lifetime of another. The 64 MiB limit covers completed
buffers; active response pages, conversions, array metadata and batches returned to the
application remain outside it. Accounting adds a temporary vector of buffer identities and
sorts it; the resident small-result reader does not use this buffered path.

## Storage Read wire backpressure

A lazy SDK-stream fixture measures how many 8 MiB responses the receiving stage materializes while
its one-slot queue is full, with and without reserving the queue slot before polling the SDK:

| Receiving stage with a stalled decoder | Materialized responses | Retained payload bytes |
| --- | ---: | ---: |
| Receive, then wait to enqueue | 2 | 16,777,216 |
| Reserve, then receive | 1 | 8,388,608 |

The fixture allocates each response only when the SDK stream is polled. It verifies that draining
the queue permits the next response, and that cancellation or decoder closure stops reception
both while waiting for queue space and while awaiting a response. Reproduce with:

```sh
cargo test --locked --lib storage_read::tests::a_full_wire_queue_does_not_materialize_another_response
cargo test --locked --lib storage_read::tests::a_stopped_decoder_releases_a_pending_wire_response
```

These are deterministic materialized-payload measurements, not process RSS or throughput
measurements. The bound is per stream and excludes a response already taken by the decoder,
completed Arrow batches and buffering inside the SDK/HTTP transport. Receiving the queued
response still overlaps decoding the previous one.

## Sliced boolean IPC encoding

The IPC writer copies a non-byte-aligned bitmap's selected bytes once, then shifts the
bounded destination slice. This lets the compiler vectorize both boolean values and validity
bitmaps without growing the output vector for each byte. The last partial byte is still masked,
including when the selected range reaches the end of its backing buffer.

The `boolean/encode-sliced` rows of `ipc` compare growing the output vector for each byte with this
bounded copy. Each fixture is a nullable boolean column sliced at bit offset one, and the table
gives the median and range of three reports, including planning and serialization:

| Rows | Before, µs (range) | After, µs (range) | Allocation calls, both | Requested bytes, both |
| ---: | ---: | ---: | ---: | ---: |
| 1,024 | 0.374 (0.374–0.387) | 0.189 (0.176–0.275) | 2 | 472 |
| 65,536 | 14.043 (14.032–14.056) | 1.005 (1.004–1.193) | 2 | 16,600 |
| 1,048,576 | 221.622 (218.198–221.814) | 14.296 (14.291–16.223) | 2 | 262,360 |

The million-row encoding is 15.5 times faster in these local measurements, with unchanged allocation
counts and requested bytes. The writer tests cover every non-byte-aligned start within four bytes,
empty and partial bitmaps, byte and vector boundaries, exact buffer ends and extra backing bytes.
Existing golden and Arrow IPC oracle tests continue to check the serialized layout:

```sh
cargo test --locked --lib arrow_ipc_bigquery::writer::tests::
```

## Foundry validation build cache

The [Driver Foundry validation suite](../integration/foundry-validation/README.md) reuses the
checkout's Cargo `target/` by default. It still builds and installs a new wheel each time; Cargo checks whether
native code needs rebuilding.
`CARGO_TARGET_DIR` selects a different cache.

On 2026-10-23, Linux x86-64, Rust 1.99.1 and uv 0.13.5, building the same checkout with a fresh
target directory and then reusing it produced these wheel-build wall times:

| Cargo artifacts | Wheel build |
| --- | ---: |
| Empty target directory | 84.591s |
| Reused, run 1 | 0.982s |
| Reused, run 2 | 0.957s |
| Reused, run 3 | 0.993s |

The warmed median was 0.982s. These measurements include packaging and native-library validation,
but exclude environment synchronization, dataset creation, pytest, and dataset cleanup. Dependency
downloads were already cached. The live Foundry suite ran concurrently, so these are local build
measurements, not a controlled end-to-end suite speedup. Source changes still incur recompilation.

Reproduce from the repository root on Linux; keep the same directory across all four builds:

```sh
timing_dir=$(mktemp -d)
for pass in cold warm1 warm2 warm3; do
    /usr/bin/time -f '%e' -o "$timing_dir/$pass.seconds" \
        env -u BIGQUERY_ADBC_BUILD_LIBRARY -u BIGQUERY_ADBC_LIBRARY \
        CARGO_TARGET_DIR="$timing_dir/target" \
        uv build --wheel --out-dir "$timing_dir/wheel" python \
        >"$timing_dir/$pass.log" 2>&1
done
cat "$timing_dir/"*.seconds
```

On the same date, the expanded unfiltered suite against BigQuery in `example-project` (`US`)
passed 566 tests and 3 subtests with 86 expected skips. Pytest took 3443.71s (57m24s); the wrapper,
including the cached wheel build, dataset creation and cleanup, took 3452.38s. The slowest cases
were stream binding: INT32 with multiple batches (126.08s), INT16 with interspersed empty batches
(70.90s), and INT16 with one batch (65.39s). Each bound parameter row submits a separate BigQuery
job. This is one live runtime sample; only wheel builds were measured both cold and warm.

```sh
PYTEST_ADDOPTS='-q --durations=15' \
  cargo test --locked --test clients foundry::suite_passes -- --ignored
```

Use `--collect-only` and the offline fixture-lint checks for discovery and fixture editing, and
`-k` selections for focused live validation. The complete suite still validates the committed
collection and skip baselines; see the [Foundry guide](../integration/foundry-validation/README.md).

## Rejected optimizations

Approaches that were tried and did not help, kept so they are not retried without a new reason.
These are notes rather than reproducible evidence: the rejected code is not in this tree, so
reopening one means rebuilding it and measuring as described under [Reproducing](#reproducing).

- Converting REST rows a column at a time ran fewer instructions than converting them in row order
  in `wide/convert/1024`, and took longer, because regrouping cells by column moves the page
  through memory again. The decoder produces one cell vector per row, which is why the cells need
  regrouping.
- A flat per-page cell slab, in place of a cell vector per row, removed about one allocation per
  row, but typed decode and convert slowed slightly and requested bytes rose.
- Selecting Storage Read by total row count reread rows that a complete first response already
  held: 100,000 narrow rows took longer that way than staying on REST.
- Allowing at most three queued Storage Write requests took much longer for 200 batches of 1,024
  rows over a residential uplink, because that left the stream waiting on round trips. Appends
  queue up to 30 MB or 1,000 requests instead.
  `scripts/run-one-live-test.sh bulk_ingests_many_small_batches --nocapture` prints the elapsed
  time for that workload.
- A full-sized first Storage Write append took longer for one 500,000-row batch than aiming for
  1 MB. That append is sent alone while a new table may still be invisible, so the rest of the
  upload waits on its round trip.
