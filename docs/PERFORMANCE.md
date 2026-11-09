# Performance

Local benchmarks cover REST decoding, result delivery, ingest normalization and Storage API IPC;
live benchmarks time `dbt build` with [dbt 2.0](DBT.md). `example-project` stands in for the test
project. Library size is in [BINARY-SIZE.md](BINARY-SIZE.md).

## Reproducing

```sh
cargo bench --locked --features benchmark --bench rest_rows   # REST result pages
cargo bench --locked --features benchmark --bench query_result # result scheduling and delivery
cargo bench --locked --features benchmark --bench normalize   # ingest normalization
cargo bench --locked --features benchmark --bench ipc         # Storage Read and Write IPC
uv run --directory integration/dbt --locked benchmark.py      # live dbt workload, needs BigQuery access
```

Each native harness prints, per case, the median of seven samples and the allocation calls and
requested bytes made through the process allocator, including worker threads. Input preparation,
network I/O and server time are outside every measured region.

- `rest_rows` feeds one result page of BigQuery JSON at 1, 32 and 1,024 rows. `narrow` (2 columns)
  and `wide` (16) alternate INTEGER and STRING; `typed` has one column per non-trivial decoder:
  TIMESTAMP, DATE, TIME, DATETIME, NUMERIC, BIGNUMERIC, BYTES, INTERVAL, a three-field STRUCT and
  a repeated INT64. `decode` is the `serde_json` pass into positional rows; `convert` is
  `rest_rows_to_record_batch` on a decoded page.
- `query_result` measures request serialization of an already-built plan for 65-, 1,024- and
  65,536-byte SQL (`query/request-*`), planning from stored SQL (`query/planning-*`), and the
  production response decoder and reader at 0, 1, 32 and 1,024 rows of a dbt data-test result (an
  integer and two booleans). Fixtures include a default dataset, location and dbt invocation label.
  `decode` includes operation tracking, the conversion permit and task scheduling; `read` starts
  from a decoded response and covers schema conversion, reader construction and draining; `complete`
  is both. The warmed runtime and anonymous client make no network requests. Work spans runtime
  threads, so do not pin it to one CPU when measuring scheduling.
- `normalize` runs 2-column (`narrow`) and 16-column (`wide`) batches at 1 and 1,024 rows:
  `canonical` (validation only), `calendar` (Date32, TimestampMicrosecond), `nested` (calendar
  columns under three struct levels), `list-window` (sliced DATE/TIMESTAMP lists, also 65,536
  rows), `coercing` (Int32 to Int64, LargeUtf8 to Utf8), `list-gather` (sliced lists with values
  behind NULL entries) and `large-offsets` (sliced LargeUtf8/LargeBinary with NULL, empty, ASCII
  and non-ASCII values, also 65,536 rows). Dictionary fixtures cover ascending and scattered keys
  with sparse null runs, and scattered keys alternating with nulls, at 1, 1,024 and 65,536 rows.
- `ipc` decodes a Storage Read record batch (aligned, misaligned, and `LZ4_FRAME` as BigQuery sends
  it: one frame of independent 64 KiB blocks per buffer, stored blocks where LZ4 cannot shrink) and
  encodes a Storage Write message. `scalar` has a nullable INT64 and a non-null STRING; `nested` and
  `deep` wrap them in one and three levels of LIST<STRUCT>. Sliced encode cases keep nonzero
  offsets at 1,024 and 65,536 rows.

Measuring a change:

- Compare before and after binaries in alternating runs. Pin CPU-only cases (`taskset -c 3`); let
  `query_result` use several CPUs. Take the binary path from the `executable` field of
  `cargo bench --locked --features benchmark --no-run --message-format=json`; it changes whenever
  `Cargo.toml` does.
- On a loaded machine count instructions: `BENCH_COUNT=wide/convert/1024 perf stat -e
  instructions:u <binary>` runs one case 1,000 times. Counts include fixture setup and harness
  overhead, and a `convert` count includes its decode (subtract `decode`). Counts miss memory
  traffic; confirm with pinned timings (see column-at-a-time conversion under
  [rejected optimizations](#rejected-optimizations)).
- Check timings as well as allocations; several changes below cut time with identical allocations.
- The 1,024-row `coercing` cases sit in a ±12% code-alignment band (an unrelated dependency's
  `opt-level` moved them about 12% with identical `.text` size). Compare against a control build
  that changes only `-C metadata`.

## Results

Figures are historical comparisons, not necessarily this checkout; to reproduce one, build both
source versions on the same machine, and record CPU and toolchain with new measurements. Unless
stated otherwise: Rust 1.99.1, locked release builds, Linux x86-64, AMD Ryzen 9 7950X3D pinned to
CPU 3, median of three alternating before/after runs of seven samples each. Affinity does not
exclude other host activity. Native results exclude network and BigQuery work and establish no
live dbt or upload speedup. Requested bytes include freed intermediates; ownership tests measure
retained buffers; neither measures RSS, allocator retention or caller-held buffers.

### Query results

Inline response decoding (JSON up to 4 KiB) and a resident reader (a complete page of at most 32
rows and 4 KiB converts on first read, without a producer task or queue) keep conversion permits,
cancellation and the host-worker handoff, since a tiny page can inherit a large schema; errors
still arrive during iteration. `query/complete`, one row, pinned to CPUs `0-7`, median (range) of
three reports:

| Implementation | Complete local handling | Allocation calls | Requested bytes |
| --- | ---: | ---: | ---: |
| Baseline | 25.92 µs (24.81–29.14) | 109 | 18,944 |
| Inline response decode | 18.68 µs (18.15–18.87) | 108 | 18,496 |
| Inline decode and resident reader | 3.43 µs (3.24–5.36) | 94 | 10,040 |

Responses are 335, 380, 1,807 and 47,441 JSON bytes at 0, 1, 32 and 1,024 rows. The 32-row case
fell from 33.16 to 10.79 µs. The buffered 1,024-row control ranged 270–276 µs before and 273–436
µs after (final sample 273 µs): no large-result change is established. The one-row saving is
about 0.6 ms across 26 data tests before concurrency.

Three alternating pairs of dbt `2.0.9` rebuilding existing relations (eight threads,
`example-project` in `US`, preparation untimed; all six verified 13 CTAS jobs, 26 SELECT jobs,
zero cache hits and the expected signature):

| Library | Median dbt wall | Range | Median summed BigQuery service time |
| --- | ---: | ---: | ---: |
| Before | 8.86s | 8.51–11.00s | 17.11s |
| After | 8.61s | 8.19–8.70s | 15.86s |

Paired after/before median `0.963` (`0.783`–`0.982`); the small sample and changing service time
and host load establish no whole-build speedup.

Later `query/complete` allocation cuts, as calls / requested bytes:

| Rows | Before | Job client deferred | Runtime bridge stack-pinned | Schema conversion |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 73 / 7,160 | 70 / 6,962 | 69 / 5,346 | 65 / 5,274 |
| 1 | 94 / 10,040 | 91 / 9,842 | 89 / 7,458 | 85 / 7,386 |
| 32 | 128 / 14,608 | 125 / 14,410 | 123 / 12,026 | 119 / 11,954 |
| 1,024 (buffered) | 1,140 / 174,512 | 1,140 / 174,512 | 1,133 / 172,674 | 1,129 / 172,602 |

- Job client deferred: the REST job-client wrapper is built only for metadata lookup or pagination.
- Runtime bridge stack-pinned: the synchronous bridge stack-pins its type-erased future. Each
  decode avoids one allocation; resident permits and buffered EOF joins avoid another; empty
  readers skip the runtime. The 1,024-row comparison started from 1,135 / 174,322. The library
  grew 1,088 bytes (0.013%). Check with `cargo test --locked --lib runtime::tests::`.
- Schema conversion: depth is checked during conversion and scalar type names are borrowed until
  metadata insertion. Serialization and response decoding are unchanged.

Concurrent builds prevented timing the job-client and schema-conversion changes.

Caching the job-client wrapper with the database configuration took warmed `query/client` from
3 allocations / 198 bytes to 0 / 0. One wrapper and URL stay resident per configuration
generation; existing connections keep their generation after option changes, and request-local
dataset resolvers stay outside the cache.

Statements, plans and lazy parameter readers share immutable SQL (replacing it leaves open
readers' snapshots intact), and request serialization reserves SQL length plus 512 bytes
(escaping, parameters or labels can still grow it). Calls / requested bytes, planning pinned to
CPUs 8–15, serialization untimed because of host activity:

| SQL bytes | `query/planning-*` before → after | `query/request-*` before → after |
| ---: | ---: | ---: |
| 65 | 7 / 268 → 6 / 180 | 3 / 896 → 1 / 577 |
| 1,024 | 7 / 1,220 → 6 / 180 | 3 / 3,230 → 1 / 1,536 |
| 65,536 | 7 / 65,732 → 6 / 180 | 3 / 196,766 → 1 / 66,048 |

Check snapshot lifetime with `cargo test --locked --lib statement::tests::execution::`.

Resident results try to take a conversion permit before entering the runtime, keeping the
cancellable wait and worker handoff. Three alternating pairs pinned to CPUs `8-15`:

| Case | Before ns | After ns | Allocation calls / bytes (unchanged) |
| --- | ---: | ---: | ---: |
| `query/read`, 1 row | 1,539 (1,532–1,583) | 1,432 (1,415–1,451) | 75 / 5,631 |
| `query/complete`, 1 row | 2,881 (2,858–2,964) | 2,778 (2,754–2,791) | 89 / 7,458 |
| `query/read`, 32 rows | 2,550 (2,514–2,569) | 2,418 (2,412–2,485) | 75 / 5,879 |
| `query/complete`, 32 rows | 9,654 (9,607–9,684) | 9,589 (9,422–9,716) | 123 / 12,026 |

Controls that skip this path: zero-row read 1,297 → 1,313 ns, buffered 1,024 rows 55,736 →
57,470 ns. Process-wide instructions for 1,000 `query/read/1` calls (three pairs, including
preparation and startup): 145,595,441 → 143,813,619.

### Ingest normalization

**Decimal buffer reuse.** Decimal128/256 validation reuses value and null buffers when width and
written scale match, keeping precision checks. `decimal-precision` and `decimal-scale` at 65,536
rows, columns alternating Decimal128(38, 9) and Decimal256(76, 38), one null per sixteen rows,
nonzero slice offsets:

| Columns | Allocation calls before / after | Requested bytes before / after |
| --- | ---: | ---: |
| 2 | 7 / 1 | 3,146,096 / 32 |
| 16 | 49 / 1 | 25,168,768 / 256 |

The remaining allocation holds column references. Unpaired timings under build/fuzz load
establish no throughput gain; two instruction-count pairs over 1,000 calls rose 0.6–0.7%.

**Nullable dictionary expansion.** Keys and nulls are gathered by consecutive spans instead of
per row when any key is null. `dictionary-*` at 65,536 rows, INT64 and STRING values alternating,
input sliced by one row:

| Dictionary keys | Columns | Before, ms | After, ms |
| --- | ---: | ---: | ---: |
| Ascending runs, four nulls per 32 rows | 2 | 2.787 | 1.474 |
| Ascending runs, four nulls per 32 rows | 16 | 14.601 | 4.572 |
| Scattered keys, four nulls per 32 rows | 2 | 1.688 | 1.474 |
| Scattered keys, four nulls per 32 rows | 16 | 14.408 | 12.887 |
| Scattered keys, alternating nulls | 2 | 1.280 | 1.347 |
| Scattered keys, alternating nulls | 16 | 10.246 | 10.806 |

Allocations are unchanged (two-column runs/scattered: 32 calls, 4,932,496 bytes; alternating: 31,
2,835,344). Alternating nulls cost about 5–6% more; no null-density heuristic was added, and
single-row valid-span and run-initialization special cases weakened the other gains. Two-column
user instructions over 1,000 iterations, including harness, preparation and drops: ascending
37.513→9.483 billion, scattered 37.512→32.850, alternating 28.789→30.263. Check against Arrow's
`take` with `cargo test --locked --lib types::cast::tests::`.

**Large-offset byte columns.** Narrowing LargeUtf8/Binary offsets preallocates one vector per
column after validating the span, keeping payload buffers shared. `large-offsets` versus
incremental growth:

| Rows | Shape | Before µs | After µs | Allocation calls before → after | Requested bytes before → after |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,024 | narrow | 5.108 | 4.277 | 25 → 7 | 33,152 → 8,616 |
| 1,024 | wide | 41.475 | 34.231 | 193 → 49 | 265,216 → 68,928 |
| 65,536 | narrow | 418.285 | 275.058 | 37 → 7 | 2,097,536 → 524,712 |
| 65,536 | wide | 3,629.338 | 3,189.348 | 289 → 49 | 16,780,288 → 4,197,696 |

65,536-row ranges: narrow 414.077–418.755 / 263.052–282.859 µs, wide 3,577.868–3,651.120 /
3,178.662–3,210.377 µs (75% fewer bytes, 34.2% / 12.1% less time). The 1,024-row `coercing/wide`
fixture fell from 20.303 to 17.411 µs, 121 to 49 calls and 199,616 to 101,472 bytes; canonical,
calendar and list-gather wide controls kept their allocations and stayed within 0.7%. Three
paired instruction counts for 1,000 wide operations fell from about 58.43 to 49.10 billion
(16.0%), including fixture construction. Check with `cargo test --locked --lib types::cast::tests::`.

**Sliced list windows.** Canonical sliced lists validate only referenced child values, keeping
offsets and translating error locations by the window origin. One paired `list-window` run
(concurrent builds make timings indicative):

| Columns / rows | Before, ns | After, ns | Allocation calls before → after | Requested bytes before → after |
| --- | ---: | ---: | ---: | ---: |
| 2 / 1,024 | 2,168 | 886 | 6 → 2 | 8,536 → 224 |
| 16 / 1,024 | 17,065 | 6,909 | 48 → 16 | 68,288 → 1,792 |
| 2 / 65,536 | 133,329 | 47,117 | 6 → 2 | 524,632 → 224 |
| 16 / 65,536 | 1,001,822 | 370,984 | 48 → 16 | 4,197,056 → 1,792 |

The remaining allocations describe child slices. Check with
`cargo test --locked --lib ingest::normalize::`.

### Storage Write encoding

`encode-sliced` rows of `ipc`, identical fixtures, nothing else running. Two successive changes:
copying and rebasing nonzero STRING/BYTES/LIST offsets in place after validation (replacing
per-offset extension and checked subtraction), then validating against the smallest offset so the
check vectorizes (the minimum must be at least the nonnegative first offset, so rebasing cannot
overflow; failures precede body materialization). Allocations were unchanged throughout.

| Rows | Shape | Original µs | Rebase in place µs | Min-offset validation µs | Allocation calls / bytes |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,024 | scalar | 1.496 | 1.143 (1.201) | 0.821 | 3 / 22,024 |
| 1,024 | nested | 3.936 | 2.898 (2.961) | 1.977 | 10 / 48,688 |
| 1,024 | deep | 16.244 | 11.419 (11.667) | 6.643 | 33 / 205,512 |
| 65,536 | scalar | 80.125 | 59.492 (60.883) | 41.123 | 3 / 1,384,840 |
| 65,536 | nested | 216.695 | 156.407 (159.263) | 97.141 | 10 / 3,032,368 |
| 65,536 | deep | 985.749 | 688.309 (703.112) | 392.241 | 33 / 12,914,376 |

The second comparison re-measured its baseline (in parentheses): reductions were 23.6–30.2% for
the first change and 31.6–44.2% for the second. 65,536-row ranges, first change: scalar
79.652–80.199 / 59.434–59.619, nested 216.604–216.832 / 156.330–156.552, deep 984.940–988.182 /
687.386–688.561 µs; second change, with visible jitter from other host work: scalar 60.103–60.967
/ 40.986–53.750, nested 157.576–159.611 / 96.855–100.296, deep 693.673–706.400 / 391.180–404.297
µs. Unsliced 1,024-row control medians: first change within 1.5% (scalar 0.573→0.576, nested
1.111→1.113, deep 3.558→3.507 µs); second change scalar 0.584→0.565, nested 1.135→1.118, deep
3.651→3.551 µs, with no unsliced speedup claimed. Three runs of 1,000 deep 65,536-row encodes fell from 11,892,573,353 to 4,141,286,435
user instructions (65.2%) with the second change, including fixture construction. The largest deep
fixture exceeds one AppendRows request and measures the codec without the chunk encoder's limits.

**Sliced booleans.** Copying a bitmap's selected bytes once and shifting in place vectorizes
without per-byte growth; partial final bytes stay masked. `boolean/encode-sliced`, nullable column
at bit offset one, including planning and serialization:

| Rows | Before, µs (range) | After, µs (range) | Allocation calls, both | Requested bytes, both |
| ---: | ---: | ---: | ---: | ---: |
| 1,024 | 0.374 (0.374–0.387) | 0.189 (0.176–0.275) | 2 | 472 |
| 65,536 | 14.043 (14.032–14.056) | 1.005 (1.004–1.193) | 2 | 16,600 |
| 1,048,576 | 221.622 (218.198–221.814) | 14.296 (14.291–16.223) | 2 | 262,360 |

Check bitmap boundaries and golden layouts with
`cargo test --locked --lib arrow_ipc_bigquery::writer::tests::`.

### Memory bounds

Ownership regressions; run each with `cargo test --locked --lib <filter>`.

| Bound | Filter |
| --- | --- |
| The dataset-location cache keeps the newest 32 insertions across a database's connections (an evicted dataset repeats `datasets.get`); 4,096 inserts leave 32, with evicted entries released. | `clients::tests::` |
| Result wrappers release their inner reader on EOF, error or observed cancellation, keeping the schema: a one-row INT64 slice of an 8,388,608-byte buffer has only the test's reference before `next()` returns (formerly two). Cancellation frees queued data at the caller's next poll. | `operation_readers_release_retained_buffers`, `cancellation::tests::` |
| A two-row Int64 parameter slice's 8,388,608-byte buffer is released before the final parameter row; empty batches before the next source callback. | `bound_parameter_batches` |
| Storage Write drops consumed batches; written or discarded IPC plans release buffers but keep scratch capacity. A 12-row mock ingest holds no reference to a two-row slice's 8 MiB buffer before the next source read, including EOF. | `the_first_ingest_batch_is_released_before_reading_more_input`, `written_and_discarded_plans_release_their_input_buffers` |
| A 1,048,576-row Float32 input normalizes to an 8,388,608-byte Float64 allocation, released before append or stream-creation retry, on success and rejection; the IPC payload still round-trips. | `encoded_chunks_release_their_normalized_window` |

**Retained result buffers.** Completed batches are charged each backing allocation once per batch,
including nested buffers and validity bitmaps, against the former logical-length charge:

| Fixture | Retained buffer bytes | Previous charge | Retained-capacity charge |
| --- | ---: | ---: | ---: |
| Nine 16-row INT64 columns sharing one 8 MiB allocation | 8,388,608 | 1,152 | 8,388,608 |
| One INT64 row in an 8 KiB allocation | 8,192 | 8 | 8,192 |

A sliced external response buffer is charged its full 8 MiB; an 8 KiB fixture fills an 8 KiB
budget and blocks a second batch until release; a row over 64 MiB proceeds under the whole-budget
clamp (`pipeline::tests::`). Shared buffers are charged independently per batch so permits have
independent lifetimes. The 64 MiB budget excludes active pages, conversions, array metadata and
returned batches. Accounting sorts a temporary vector of buffer identities; resident readers skip
it.

**Retained REST conversion inputs.** Pending conversions charge owned row, cell and nested vectors
and strings, including spare capacity, capped at 64 MiB (one batch may cross); resident pages and
arrays being built are excluded. A 16,384-row fixture with an 8 MiB STRING in its first row: the
first 1,024-row batch owns 8,446,048 bytes, which the former page-average wire charge put at
543,744 (15.5 times too small). Eight such batches now fill the window; their former charges
totalled 4,349,952. Filters: `a_skewed_page_charges_the_wide_batch_its_retained_allocation`,
`retained_bytes_include_nested_allocations_and_spare_capacity`. Accounting and worker handoff,
three pairs pinned to CPUs 0–7 under concurrent host activity:

| Case, 1,024 rows | Before, µs | After, µs | Allocation calls before → after | Requested bytes before → after |
| --- | ---: | ---: | ---: | ---: |
| `query/read` | 63.631 (61.425–64.301) | 71.246 (66.748–73.089) | 83 → 84 | 23,095 → 23,959 |
| `query/complete` | 275.117 (271.156–277.929) | 279.864 (273.156–281.319) | 1,129 → 1,130 | 172,602 → 173,466 |

Resident one-row controls skip it: 71 / 5,559 reading and 85 / 7,386 complete.

**Storage Read wire backpressure.** Reserving the one-slot queue before polling the SDK stream
bounds the responses materialized behind a stalled decoder (8 MiB responses):

| Receiving stage | Materialized responses | Retained payload bytes |
| --- | ---: | ---: |
| Receive, then wait to enqueue | 2 | 16,777,216 |
| Reserve, then receive | 1 | 8,388,608 |

`storage_read::tests::a_stopped_stage_polls_no_further_responses` checks that a full queue polls no
further response and that cancellation or decoder closure releases a pending poll. The bound
excludes the decoder's current response, completed batches and SDK/HTTP buffering.

### Live dbt workload

dbt `2.0.9`, default 13-model, 26-test workload (50,000 base rows, eight threads, fresh datasets),
`example-project` in `US`, three rounds alternating dbt's opt-in [Foundry driver](DBT.md#installing)
`0.21.0.dev+dbt0.1.5` with this driver before the metadata change below:

| Driver | Median wall | Range | Median jobs/s | Median warehouse span |
| --- | ---: | ---: | ---: | ---: |
| Downloaded Foundry | 23.92s | 23.39–28.62s | 1.63 | 20.01s |
| This driver, before the metadata change | 8.64s | 8.39–10.31s | 4.51 | 6.58s |

Paired local/Foundry wall ratio `0.359` (64.1% lower). Every invocation ran 13 CTAS and 26 SELECT
jobs, no cache hits, expected signature. Median summed service time: 18.42s Foundry, 16.18s local.
The historical default-driver result is in
[integration/dbt/PERFORMANCE.md](../integration/dbt/PERFORMANCE.md#historical-default-driver-result).

The metadata change makes `GetTableSchema` request
[`tables.get(view=BASIC)`](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables/get#TableMetadataView)
instead of `FULL`, which adds storage accounting dbt does not use; `GetStatistics` still requests
`STORAGE_STATS`. 24 alternating pairs per relation, same field selectors, one warmed HTTP session,
all projected responses equal:

| Relation | FULL median | BASIC median | Median paired saving |
| --- | ---: | ---: | ---: |
| Partitioned, clustered table | 282.8ms | 200.4ms | 63.0ms |
| Logical view | 289.6ms | 181.3ms | 75.6ms |
| Materialized view | 254.7ms | 207.6ms | 52.9ms |

A second 24-pair run with `benchmark_metadata.py` saved 57.6, 40.1 and 35.8ms respectively, also
with equal configurations. These exclude Rust transport, and concurrent lookups' savings do not add
to dbt wall time. The live `table_metadata_describes_what_defines_each_relation` test covers
tables, views, materialized views and external tables.

Five pairs with `--existing`, alternating order, each verifying 39 jobs, zero cache hits and the
signature:

| Library | Median wall | Range | Median summed BigQuery service time |
| --- | ---: | ---: | ---: |
| Before | 9.82s | 8.49–16.52s | 19.72s |
| After | 9.88s | 8.53–10.86s | 17.93s |

Paired after/before median `0.931` (`0.546`–`1.181`; three improved, two regressed): no end-to-end
speedup is established, only cheaper configuration lookups. Three rounds of the updated driver
against Foundry, both with `--existing`, same checks:

| Driver | Median wall | Range | Median jobs/s | Median warehouse span |
| --- | ---: | ---: | ---: | ---: |
| Downloaded Foundry | 25.39s | 24.86–28.16s | 1.54 | 18.36s |
| This driver | 10.23s | 9.78–17.43s | 3.81 | 8.36s |

Paired local/Foundry ratio `0.403` (59.7% lower); this compares drivers, not the metadata change.

```sh
uv run --directory integration/dbt --locked benchmark.py
uv run --directory integration/dbt --locked benchmark.py --existing
uv run --directory integration/dbt --locked benchmark_metadata.py --include-materialized-view
```

See the [benchmark guide](../integration/dbt/PERFORMANCE.md) for workload controls and
`--local-library`. Variance is substantial; timings have no CI threshold.

#### Repeated dataset discovery

dbt `2.0.9` listed datasets twice per connection: schema registration without a filter, then a
table-depth relation-cache lookup, because dataset underscores are ADBC wildcards. Neither was a
retry, and dbt set no options. `bigquery.get_objects.disable_wildcards=true` removed the second
listing for populated relations; both diagnostic builds passed 39 jobs with no cache hits and the
expected signature, without establishing a whole-build speedup. Empty datasets still need
`datasets.get` after `tables.list` to tell them from missing ones. Mock test:

```sh
cargo test --locked --all-features disabling_wildcards_keeps_unfiltered_dataset_and_table_listings
```

[dbt commit `7f6844449`](https://github.com/dbt-labs/dbt/commit/7f6844449b13b920dc9ea4da49806d1d835f718b)
enables literal filters best-effort on dbt `main`; the `2.0.9` binary lacks it. The
[canonical](DBT.md#installing) `0.21.0.dev+dbt0.21.19` and opt-in Foundry `0.21.0.dev+dbt0.1.5`
drivers return `NOT_IMPLEMENTED` for `disable_wildcards` and `skip_table_metadata`.

### Result pages and Storage Read

Seconds to read a whole result in one Rust process against `example-project` with
`bigquery.query.disable_query_cache`, two rounds per build, alternating. Narrow rows are
`SELECT n FROM UNNEST(GENERATE_ARRAY(1, N)) AS n ORDER BY n`; padded rows add
`REPEAT('x', 500) AS padding` (about 20,000 per 10 MB page). "Before" sent `jobs.query` without
`maxResults` (a first response of up to 350,000 narrow rows), paged by 10,000 and used Storage Read
from three pages left; "after" used 100,000-row responses and Storage Read from two pages or 80,000
rows left. That threshold was later replaced by the
[continuation-token rule](#choosing-storage-read-for-every-paginated-result).

| Result | Before | After | REST only, before | REST only, after |
| --- | ---: | ---: | ---: | ---: |
| 1,000 narrow rows | 0.45–0.56 | 0.49–0.51 | | |
| 50,000 narrow rows | 0.97–1.25 | 1.10–1.21 | | |
| 100,000 narrow rows | 2.45–2.68 | 2.47–2.75 | | |
| 200,000 narrow rows | 4.70–5.16 | 3.99–5.52 | 4.49–5.92 | 4.63–4.68 |
| 600,000 narrow rows | 8.49–8.72 | 4.73–5.22 | 19.89–20.42 | 13.57–14.49 |
| 60,000 padded rows | 3.33–3.68 | 3.55–3.63 | 4.76–4.85 | 3.84–4.37 |

Same setup, 100,000-row pages, Storage Read forced at any size versus
`bigquery.query.use_storage_api_disabled_client=true`:

| Result | Pages after the first | Storage Read | REST |
| --- | ---: | ---: | ---: |
| 120,000 narrow rows | 1 (20,000 rows) | 4.35–4.97 | 3.25–3.29 |
| 160,000 narrow rows | 1 (60,000 rows) | 4.50–4.56 | 4.02–4.20 |
| 200,000 narrow rows | 1 (100,000 rows) | 4.08–4.12 | 4.72–4.80 |
| 40,000 padded rows | 1 | 3.24–3.47 | 3.03–3.12 |
| 50,000 padded rows | 2 | 3.45–3.71 | 3.88–4.09 |
| 6,000 rows of 3,000 bytes | 1 | 2.67–3.33 | 2.23–2.42 |
| 10,000 rows of 3,000 bytes | 2 | 3.08–4.61 | 2.89–2.99 |

Storage Read's `jobs.get` and session setup took about a second before rereading the whole result.
A REST page cost a round trip plus about 20 µs per narrow row: for 600,000 narrow rows the first
`jobs.query` response took 8.4–8.5 s holding 350,000 rows and 3.1–3.2 s holding 100,000.

### Choosing Storage Read for every paginated result

A continuation token on the first completed response selects Storage Read before any rows are
delivered, since a first page's average width cannot rule out a later row over REST's limit.
Complete first responses stay on REST; explicit disabling or an unavailable session keeps REST
fallback.

`example-project` in `US`, `adbc_driver_manager` 1.12.0, PyArrow 26.0.0, default-feature release
`cdylib` builds of the row-count/width threshold (baseline) and the continuation-token rule. Each
connection was warmed by one query, then five pairs ran in one Python process, alternating order,
query cache disabled; timings include execution, fetching every batch and checking counts, sums
and padding lengths. The queries scan no tables.

| Result | Baseline, median (range), s | Continuation-token rule, median (range), s | Median change |
| --- | ---: | ---: | ---: |
| 1,000 narrow rows, complete first response | 0.437 (0.419–0.490) | 0.440 (0.386–0.500) | +0.003 s |
| 120,000 narrow rows, one further REST page | 3.574 (3.415–3.936) | 4.011 (3.694–4.585) | +0.437 s, +12.2% |
| 6,000 rows with 3,000-byte padding, one further REST page | 2.450 (2.351–2.547) | 2.981 (2.788–3.073) | +0.531 s, +21.7% |

Debug diagnostics confirmed all ten baseline paginated reads used REST and all ten candidate reads
Storage Read; the complete-response control stayed on REST, and its overlapping ranges establish
no change. Small paginated results pay this setup for the guarantee that a later wide row cannot
fail REST after earlier pages were delivered. The live regression reads 2,000 rows then one of
thirty 800,000-byte strings (24 MB, beyond REST) and checks every row and string:

```sh
scripts/run-one-live-test.sh reads_a_later_row_too_large_for_rest_through_storage_read
```

### Foundry validation build cache

The [Foundry suite](../integration/foundry-validation/README.md) builds a new wheel each run,
reusing Cargo's `target/` unless `CARGO_TARGET_DIR` overrides it. 2026-10-23, Linux x86-64, Rust
1.99.1, uv 0.13.5, same checkout, dependencies cached, live suite running concurrently:

| Cargo artifacts | Wheel build |
| --- | ---: |
| Empty target directory | 84.591s |
| Reused, run 1 | 0.982s |
| Reused, run 2 | 0.957s |
| Reused, run 3 | 0.993s |

Times include packaging and library validation, not environment sync, datasets or pytest; source
changes still recompile, and no end-to-end suite speedup is measured. From the repository root:

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

One live sample the same day: the unfiltered suite passed 566 tests and 3 subtests with 86
expected skips; pytest took 3443.71s and the wrapper (cached wheel build, datasets, cleanup)
3452.38s. Slowest were stream binding cases, since each bound row submits a BigQuery job: INT32
multiple batches 126.08s, INT16 interspersed empty batches 70.90s, INT16 one batch 65.39s.

```sh
PYTEST_ADDOPTS='-q --durations=15' \
  cargo test --locked --test clients foundry::suite_passes -- --ignored
```

## Rejected optimizations

Re-evaluating one means rebuilding the rejected code and [measuring again](#reproducing).

| Change | Result |
| --- | --- |
| Column-at-a-time REST conversion | Fewer instructions in `wide/convert/1024` but slower: regrouping per-row cell vectors moves the page through memory again. |
| A flat per-page cell slab instead of a cell vector per row | About one allocation fewer per row, but typed decode and convert slowed slightly and requested bytes rose. |
| Selecting Storage Read by total row count | Rereads rows a complete first response already held; 100,000 narrow rows took longer than staying on REST. |
| At most three queued Storage Write requests | Much slower for 200 batches of 1,024 rows over a residential uplink: the stream waits on round trips. Appends queue up to 30 MB or 1,000 requests; `scripts/run-one-live-test.sh bulk_ingests_many_small_batches --nocapture` prints the elapsed time. |
| A full-sized first Storage Write append | Slower than aiming for 1 MB for one 500,000-row batch: that append is sent alone while a new table may be invisible, and the rest waits on it. |
