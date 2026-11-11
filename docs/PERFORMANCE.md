# Performance

Local benchmarks cover REST decoding, result delivery, ingest normalization and Storage API IPC;
live benchmarks time `dbt build` with [dbt 2.0](DBT.md). `example-project` stands in for the test
project. Library size is in [BINARY-SIZE.md](BINARY-SIZE.md).

## Reproducing

```sh
cargo bench --locked --features benchmark --bench rest_rows    # REST result pages
cargo bench --locked --features benchmark --bench query_result # result scheduling and delivery
cargo bench --locked --features benchmark --bench normalize    # ingest normalization
cargo bench --locked --features benchmark --bench ipc          # Storage Read and Write IPC
uv run --directory integration/dbt --locked benchmark.py       # live dbt workload, needs BigQuery access
```

Each native harness prints, per case, the median of seven samples and the allocation calls and
requested bytes made through the process allocator, including worker threads. Input preparation,
network I/O and server time are outside every measured region.

- `rest_rows` feeds one result page of BigQuery JSON at 1, 32 and 1,024 rows. `narrow` (2 columns)
  and `wide` (16) alternate INTEGER and STRING; `typed` has one column per non-trivial decoder:
  TIMESTAMP, DATE, TIME, DATETIME, NUMERIC, BIGNUMERIC, BYTES, INTERVAL, a three-field STRUCT and
  a repeated INT64. `decode` is the `serde_json` pass into positional rows; `convert` is
  `rest_rows_to_record_batch` on a decoded page.
- `query_result` measures request serialization of a built plan for 65-, 1,024- and 65,536-byte
  SQL (`query/request-*`), planning from stored SQL (`query/planning-*`), the job-client lookup
  (`query/client`), and the production response decoder and reader at 0, 1, 32 and 1,024 rows of a
  dbt data-test result (an integer and two booleans, with a default dataset, location and dbt
  invocation label). `decode` includes operation tracking, the conversion permit and task
  scheduling; `read` starts from a decoded response and covers schema conversion, reader
  construction and draining; `complete` is both. The warmed runtime and anonymous client make no
  network requests.
- `normalize` runs 2-column (`narrow`) and 16-column (`wide`) batches at 1 and 1,024 rows:
  `canonical` (validation only), `calendar` (Date32, TimestampMicrosecond), `nested` (calendar
  columns under three struct levels), `coercing` (Int32 to Int64, LargeUtf8 to Utf8),
  `list-gather` (sliced lists with values behind NULL entries), and these, also at 65,536 rows:
  `list-window` (sliced DATE/TIMESTAMP lists), `large-offsets` (sliced LargeUtf8/LargeBinary with
  NULL, empty, ASCII and non-ASCII values), `decimal-precision` and `decimal-scale`, and dictionary
  keys in ascending runs (`dictionary-runs`) or scattered (`dictionary-scattered`), both with four
  nulls per 32 rows, or scattered and alternating with nulls (`dictionary-alternating`).
  `dictionary-labels` puts 200-byte labels in a first column that holds nearly every byte, beside
  BOOLEAN columns. Each `encode` row (`canonical/narrow/encode`) splits the batch into Storage
  Write requests as an append after the stream's first does: it sizes each window of rows,
  normalizes it and frames it as Arrow IPC.
- `ipc` decodes a Storage Read record batch (aligned, misaligned, and `LZ4_FRAME` as BigQuery sends
  it: one frame of independent 64 KiB blocks per buffer, stored blocks where LZ4 cannot shrink) and
  encodes a Storage Write message. `scalar` has a nullable INT64 and a non-null STRING; `nested` and
  `deep` wrap them in one and three levels of LIST<STRUCT>. `encode-sliced` cases, including
  `boolean`, keep nonzero offsets.

Measuring a change:

- Compare before and after binaries in alternating runs. Pin CPU-only cases (`taskset -c 3`), but
  let `query_result` use several CPUs, since its work spans runtime threads. Take the binary path
  from the `executable` field of `cargo bench --locked --features benchmark --no-run
  --message-format=json`; it changes whenever `Cargo.toml` does.
- On a loaded machine count instructions: `BENCH_COUNT=wide/convert/1024 perf stat -e
  instructions:u <binary>` runs one case 1,000 times. Counts include fixture setup and harness
  overhead (a `convert` count includes its decode) and miss memory traffic, so confirm them with
  pinned timings (see column-at-a-time conversion under
  [rejected optimizations](#rejected-optimizations)).
- Check timings as well as allocations; several changes below cut time with identical allocations.
- The 1,024-row `coercing` cases sit in a ±12% code-alignment band (an unrelated dependency's
  `opt-level` moved them about 12% with identical `.text` size). Compare against a control build
  that changes only `-C metadata`.

## Results

Each figure compares builds of one change and need not match this checkout; to reproduce one, build
both source versions on one machine. Unless stated otherwise: Rust 1.99.1, locked release builds,
Linux x86-64, AMD Ryzen 9 7950X3D pinned to CPU 3 (other host activity not excluded), median of
three alternating before/after runs of seven samples each. Native results exclude network and
BigQuery work, so they establish no live speedup. Requested bytes include freed intermediates;
ownership tests measure retained buffers; neither measures RSS, allocator retention or caller-held
buffers.

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
fell from 33.16 to 10.79 µs; the buffered 1,024-row control showed no established change. The
one-row saving is about 0.6 ms across 26 data tests before concurrency. Three alternating pairs of
dbt `2.0.9` rebuilding existing relations (eight threads) establish no whole-build speedup: median
wall 8.86 s before and 8.61 s after, paired ratio `0.963` (`0.783`–`0.982`).

Later `query/complete` allocation cuts, as calls / requested bytes:

| Rows | Before | Job client deferred | Runtime bridge stack-pinned | Schema conversion |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 73 / 7,160 | 70 / 6,962 | 69 / 5,346 | 65 / 5,274 |
| 1 | 94 / 10,040 | 91 / 9,842 | 89 / 7,458 | 85 / 7,386 |
| 32 | 128 / 14,608 | 125 / 14,410 | 123 / 12,026 | 119 / 11,954 |
| 1,024 (buffered) | 1,140 / 174,512 | 1,140 / 174,512 | 1,133 / 172,674 | 1,129 / 172,602 |

- Job client deferred: the REST job-client wrapper is built only for metadata lookup or pagination.
- Runtime bridge stack-pinned: the synchronous bridge stack-pins its type-erased future, saving one
  allocation per decode and another for resident permits and buffered EOF joins; empty readers
  skip the runtime. Its 1,024-row comparison started from 1,135 / 174,322. The library grew 1,088
  bytes (0.013%). Check with `cargo test --locked --lib runtime::tests::`.
- Schema conversion: depth is checked during conversion and scalar type names are borrowed until
  metadata insertion; serialization and response decoding are unchanged.

Concurrent builds prevented timing the job-client and schema-conversion changes.

Caching the job-client wrapper with the database configuration took warmed `query/client` from
3 allocations / 198 bytes to none. One wrapper and URL stay resident per configuration generation;
existing connections keep their generation after option changes, and request-local dataset
resolvers stay outside the cache.

Statements, plans and lazy parameter readers share immutable SQL (replacing it leaves open readers'
snapshots intact), and request serialization reserves SQL length plus 512 bytes (escaping,
parameters or labels can still grow it). Calls / requested bytes; host activity prevented timing
serialization:

| SQL bytes | `query/planning-*` before → after | `query/request-*` before → after |
| ---: | ---: | ---: |
| 65 | 7 / 268 → 6 / 180 | 3 / 896 → 1 / 577 |
| 1,024 | 7 / 1,220 → 6 / 180 | 3 / 3,230 → 1 / 1,536 |
| 65,536 | 7 / 65,732 → 6 / 180 | 3 / 196,766 → 1 / 66,048 |

Check snapshot lifetime with `cargo test --locked --lib statement::tests::execution::`.

Resident results try to take a conversion permit before entering the runtime, keeping the
cancellable wait and worker handoff. Three alternating pairs pinned to CPUs `8-15`, allocations
unchanged:

| Case | Before ns | After ns | Allocation calls / bytes |
| --- | ---: | ---: | ---: |
| `query/read`, 1 row | 1,539 (1,532–1,583) | 1,432 (1,415–1,451) | 75 / 5,631 |
| `query/complete`, 1 row | 2,881 (2,858–2,964) | 2,778 (2,754–2,791) | 89 / 7,458 |
| `query/read`, 32 rows | 2,550 (2,514–2,569) | 2,418 (2,412–2,485) | 75 / 5,879 |
| `query/complete`, 32 rows | 9,654 (9,607–9,684) | 9,589 (9,422–9,716) | 123 / 12,026 |

Controls that skip this path: zero-row read 1,297 → 1,313 ns, buffered 1,024 rows 55,736 → 57,470
ns. Process-wide instructions for 1,000 `query/read/1` calls (three pairs, including preparation
and startup) fell from 145,595,441 to 143,813,619.

**REST decimal cells.** NUMERIC and BIGNUMERIC cells accumulate their significant digits eighteen
at a time from one contiguous run, skipping leading zeros and appending the scale's zeros a chunk
at a time. `typed/convert` (one NUMERIC, one BIGNUMERIC and a struct's NUMERIC per row; 78
allocation calls before and after), CPU 5, three alternating runs:

| Rows | Before, ns | After, ns |
| ---: | ---: | ---: |
| 32 | 17,415–21,732 | 14,031–14,518 |
| 1,024 | 494,009–709,059 | 395,310–399,486 |

User instructions for 1,000 `typed/convert/1024` iterations fell from 23.43 to 20.91 billion, of
which decode is 13.52, so conversion alone fell 25%. Check with
`cargo test --locked --lib rows::scalars::`.

### Ingest normalization

**Decimal buffer reuse.** Decimal128/256 validation reuses value and null buffers when width and
written scale match, keeping precision checks. `decimal-precision` and `decimal-scale` at 65,536
rows, columns alternating Decimal128(38, 9) and Decimal256(76, 38), one null per sixteen rows,
nonzero slice offsets:

| Columns | Allocation calls before / after | Requested bytes before / after |
| ---: | ---: | ---: |
| 2 | 7 / 1 | 3,146,096 / 32 |
| 16 | 49 / 1 | 25,168,768 / 256 |

The remaining allocation holds column references. No throughput gain is established: timings were
unpaired and under build load, and two instruction-count pairs over 1,000 calls rose 0.6–0.7%.

**Nullable dictionary expansion.** When any key is null, keys and nulls are gathered by
consecutive spans instead of per row. For `dictionary-runs` at 65,536 rows (INT64 and STRING values
alternating, input sliced by one row), 2 columns fell from 2.787 to 1.474 ms and 16 from 14.601 to
4.572 ms, with unchanged allocations; two-column user instructions over 1,000 iterations fell from
37.513 to 9.483 billion. Check against Arrow's `take` with
`cargo test --locked --lib types::cast::tests::`.

**Direct dictionary gathers.** Keys resolve once into value indices; primitive values (numeric,
temporal and decimal) are then copied a run at a time and string or binary values row by row (or
through `MutableArrayData` when runs average eight rows or more), instead of one type-erased
`MutableArrayData` extend per run, which scattered keys make one per row. Boolean and
`FixedSizeBinary` values keep the per-run extend. Same fixtures, CPU 5:

| Dictionary keys, 65,536 rows | Columns | Before, ms | After, ms |
| --- | ---: | ---: | ---: |
| `dictionary-runs` | 2 | 1.433 | 1.447 |
| `dictionary-runs` | 16 | 4.437 | 4.402 |
| `dictionary-scattered` | 2 | 1.470 | 0.715 |
| `dictionary-scattered` | 16 | 12.699 | 5.822 |
| `dictionary-alternating` | 2 | 1.341 | 0.605 |
| `dictionary-alternating` | 16 | 10.738 | 4.871 |

Two-column calls and requested bytes: scattered 32 / 4,932,496 → 20 / 2,974,292, alternating 31 /
2,835,344 → 20 / 2,493,524, runs 32 / 4,932,496 → 30 / 5,981,104 (the index vector, 8 bytes a
row). User instructions over 1,000 two-column iterations: scattered 32.99 → 16.37 billion,
alternating 30.40 → 13.63, runs 9.63 → 10.06. Strings are checked as UTF-8 again when their array is
built, since only `unsafe` skips that; a builder-based gather avoided it for another 25% but cost
21 KB more library. The library grew 18,672 bytes (0.22%).

Through request sizing and IPC framing (`encode`), and for `dictionary-labels` (200-byte labels
behind `Int16` keys), 65,536 rows:

| Case | Before, ms | After, ms | Requested bytes before → after |
| --- | ---: | ---: | ---: |
| `dictionary-scattered/narrow/encode` | 1.744 | 1.001 | 6,860,484 → 4,902,280 |
| `dictionary-scattered/wide/encode` | 14.518 | 9.016 | 55,127,516 → 39,357,772 |
| `dictionary-labels/narrow` | 1.293 | 0.856 | 33,751,560 → 13,902,308 |
| `dictionary-labels/narrow/encode` | 1.739 | 1.401 | 47,166,988 → 27,285,492 |

User instructions over 1,000 iterations: `dictionary-scattered/narrow/encode` 36.85 → 20.23
billion, `dictionary-labels/narrow/encode` 26.46 → 23.06. Requested bytes fall because the gather
sizes its buffers from the keys up front, where `MutableArrayData` grows them as it copies.

**Large-offset byte columns.** Narrowing LargeUtf8/Binary offsets preallocates one vector per
column after validating the span, keeping payload buffers shared. `large-offsets` versus
incremental growth:

| Rows | Shape | Before µs | After µs | Allocation calls before → after | Requested bytes before → after |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,024 | narrow | 5.108 | 4.277 | 25 → 7 | 33,152 → 8,616 |
| 1,024 | wide | 41.475 | 34.231 | 193 → 49 | 265,216 → 68,928 |
| 65,536 | narrow | 418.285 | 275.058 | 37 → 7 | 2,097,536 → 524,712 |
| 65,536 | wide | 3,629.338 | 3,189.348 | 289 → 49 | 16,780,288 → 4,197,696 |

The 65,536-row ranges did not overlap. The 1,024-row `coercing/wide` fixture fell from 20.303 to
17.411 µs, 121 to 49 calls and 199,616 to 101,472 bytes; canonical, calendar and list-gather wide
controls kept their allocations and stayed within 0.7%. Three paired instruction counts for 1,000
wide operations, including fixture construction, fell 16.0%, from about 58.43 to 49.10 billion.
Check with
`cargo test --locked --lib types::cast::tests::`.

**Sliced list windows.** Canonical sliced lists validate only referenced child values, keeping
offsets and translating error locations by the window origin. One paired `list-window` run, under
concurrent builds:

| Columns / rows | Before, ns | After, ns | Allocation calls before → after | Requested bytes before → after |
| --- | ---: | ---: | ---: | ---: |
| 2 / 1,024 | 2,168 | 886 | 6 → 2 | 8,536 → 224 |
| 16 / 1,024 | 17,065 | 6,909 | 48 → 16 | 68,288 → 1,792 |
| 2 / 65,536 | 133,329 | 47,117 | 6 → 2 | 524,632 → 224 |
| 16 / 65,536 | 1,001,822 | 370,984 | 48 → 16 | 4,197,056 → 1,792 |

The remaining allocations describe child slices. Check with
`cargo test --locked --lib ingest::normalize::`.

**Dictionary request windows.** Sizing a request's window reads a string or byte dictionary's
value offsets once instead of dispatching on the value array for every row. User instructions for
1,000 `encode` operations at 65,536 rows, including fixture construction, two runs each (equal to
within 0.0001%), allocations unchanged:

| Case | Before, billions | After, billions | Change |
| --- | ---: | ---: | ---: |
| `dictionary-runs/wide/encode` | 195.09 | 147.59 | −24.3% |
| `dictionary-scattered/narrow/encode` | 40.96 | 36.76 | −10.2% |
| `dictionary-scattered/wide/encode` | 459.46 | 411.96 | −10.3% |
| `dictionary-alternating/narrow/encode` | 36.01 | 33.59 | −6.7% |

The normalization-only control `dictionary-scattered/narrow` stayed at 32.99. Expanding the
values (`expand_dictionary` in `src/types/cast.rs`) is most of what remains. Check with
`cargo test --locked --lib ingest::write::window::`.

**First request window.** A batch's first window, with no chunk before it to give a density, aims
for the request target by the bytes its rows address, rather than taking up to twice the target and
normalizing the rows past the cut again for the next chunk. Batches between one and two request
targets gain, as the 65,536-row wide fixtures are. Instructions as above; timings and allocations
from one pinned bench run each:

| Case | Instructions before → after, billions | Before → after, ms | Requested bytes before → after |
| --- | ---: | ---: | ---: |
| `dictionary-scattered/wide/encode` | 411.96 → 322.04 (−21.8%) | 24.58 → 14.33 | 71,311,372 → 55,127,516 |
| `dictionary-runs/wide/encode` | 147.59 → 134.15 (−9.1%) | 12.16 → 6.37 | 71,308,796 → 55,125,468 |
| `large-offsets/wide/encode` | 71.22 → 51.69 (−27.4%) | 3.21 → 2.47 | 20,097,476 → 18,549,572 |
| `list-window/wide/encode` | 25.41 → 16.43 (−35.3%) | 1.36 → 0.96 | 16,847,788 → 16,832,940 |

Controls stayed within 0.002%: `decimal-scale/wide/encode` at 65,536 rows (over twice the target),
`dictionary-scattered/narrow/encode` and `large-offsets/narrow/encode` at 65,536 rows (under it),
and `canonical/wide/encode` at 1,024 rows.

**Request-window cap.** Sizing walks a dictionary's keys or a list view's ranges until the bytes
pass a cap, and walks them again after each refusal. The cap is above twice the target, so a
refusal more than halves the window; an estimate from exact bytes is trusted once, after which a
refusal at least halves it. With the cap at the target, a column holding nearly every byte ended
each walk just past the target, so each pass shrank the window by a sliver: a 1,000,000-row
`Dictionary<Int16, Utf8>` of 100-byte labels took 41 s to size at the 9 MB target in a release
build; above twice the target it takes 5 passes (8 at the 1 MB first-append target). User
instructions for 1,000 `dictionary-labels` encodes of 65,536 rows, and one pinned timing run each:

| Case | Instructions, cap at target → above twice it, billions | Cap at target → above twice it, ms |
| --- | ---: | ---: |
| `dictionary-labels/narrow/encode` | 1,082.96 → 26.46 (−97.6%) | 42.77 → 1.67 |
| `dictionary-labels/wide/encode` | 98.73 → 26.63 (−73.0%) | 4.35 → 1.69 |

The other `encode` cases above are the same under either cap. Check with
`cargo test --locked --lib ingest::write::window::tests::a_window_without`.

**First batch of an append.** `create` and `replace` normalize and size the whole first batch
before changing the target, then normalize it again as it is sent; an append into an existing
table skips the first pass. For one 1,000,000-row batch shaped like a pandas frame (INT64,
FLOAT64, STRING, nanosecond TIMESTAMP and a three-value `Dictionary<Int8, Utf8>`), in a release
build, the skipped pass cost 238.0 million user instructions and the sending pass 241.1 million
(five passes each, two runs, fixture construction subtracted), so local conversion work halves.
Check with `cargo test --locked --lib ingest::tests::only_an`.

### Storage Write encoding

`ipc`'s `encode-sliced` rows over two changes: copying and rebasing nonzero STRING/BYTES/LIST
offsets in place after validation, instead of per-offset extension and checked subtraction; then
validating against the smallest offset so the check vectorizes (the minimum must be at least the
nonnegative first offset, so rebasing cannot overflow, and failures precede body
materialization). Allocations were unchanged throughout.

| Rows | Shape | Original µs | Rebase in place µs | Min-offset validation µs | Allocation calls / bytes |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,024 | scalar | 1.496 | 1.143 (1.201) | 0.821 | 3 / 22,024 |
| 1,024 | nested | 3.936 | 2.898 (2.961) | 1.977 | 10 / 48,688 |
| 1,024 | deep | 16.244 | 11.419 (11.667) | 6.643 | 33 / 205,512 |
| 65,536 | scalar | 80.125 | 59.492 (60.883) | 41.123 | 3 / 1,384,840 |
| 65,536 | nested | 216.695 | 156.407 (159.263) | 97.141 | 10 / 3,032,368 |
| 65,536 | deep | 985.749 | 688.309 (703.112) | 392.241 | 33 / 12,914,376 |

The second change re-measured its baseline (in parentheses). Reductions were 23.6–30.2% for the
first change and 31.6–44.2% for the second, whose runs showed jitter from other host work;
unsliced 1,024-row controls moved by at most 3.3%, so no unsliced speedup is claimed. Three runs of
1,000 deep 65,536-row encodes fell from 11,892,573,353 to 4,141,286,435 user instructions (65.2%)
with the second change, including fixture construction. The largest deep fixture exceeds one
AppendRows request, so it measures the codec without the chunk encoder's limits.

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
| The dataset-location cache keeps the newest 32 insertions across a database's connections; 64 inserts leave 32, with evicted entries released. | `clients::tests::` |
| Result wrappers release their inner reader on EOF, error or observed cancellation, keeping the schema: a one-row INT64 slice of an 8,388,608-byte buffer has only the test's reference before `next()` returns. Cancellation frees queued data at the caller's next poll. | `operation_readers_release_retained_buffers`, `cancellation::tests::` |
| A two-row Int64 parameter slice's 8,388,608-byte buffer is released before the final parameter row; empty batches before the next source callback. | `bound_parameter_batches` |
| Storage Write drops consumed batches; written or discarded IPC plans release buffers but keep scratch capacity. A 12-row mock ingest holds no reference to a two-row slice's 8 MiB buffer before the next source read, including EOF. | `the_first_ingest_batch_is_released_before_reading_more_input`, `written_and_discarded_plans_release_their_input_buffers` |
| A 1,048,576-row Float32 input normalizes to an 8,388,608-byte Float64 allocation, released before append or stream-creation retry, on success and rejection; the IPC payload still round-trips. | `encoded_chunks_release_their_normalized_window` |

**Retained result buffers.** Completed batches are charged each backing allocation once per batch,
including nested buffers and validity bitmaps, rather than their logical length:

| Fixture | Retained buffer bytes | Logical-length charge | Retained-capacity charge |
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
arrays being built are excluded. In a 16,384-row fixture with an 8 MiB STRING in its first row, the
first 1,024-row batch owns 8,446,048 bytes, where a page-average wire charge gives 543,744 (15.5
times too small); eight such batches fill the window, against 4,349,952 bytes under the wire
charge. Filters: `a_skewed_page_charges_the_wide_batch_its_retained_allocation`,
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

dbt `2.0.9`, default 13-model, 26-test workload (50,000 base rows, eight threads), `example-project`
in `US`, three rounds alternating dbt's opt-in [Foundry driver](DBT.md#installing)
`0.21.0.dev+dbt0.1.5` with this driver. Fresh datasets ran before the metadata change below,
`--existing` after it. Every invocation ran 13 CTAS and 26 SELECT jobs, no cache hits and the
expected signature:

| Workload | Driver | Median wall | Range | Median jobs/s | Median warehouse span |
| --- | --- | ---: | ---: | ---: | ---: |
| Fresh datasets | Foundry | 23.92s | 23.39–28.62s | 1.63 | 20.01s |
| Fresh datasets | This driver | 8.64s | 8.39–10.31s | 4.51 | 6.58s |
| `--existing` | Foundry | 25.39s | 24.86–28.16s | 1.54 | 18.36s |
| `--existing` | This driver | 10.23s | 9.78–17.43s | 3.81 | 8.36s |

Paired local/Foundry wall ratios are `0.359` fresh (median summed service time 18.42s Foundry,
16.18s local) and `0.403` with `--existing`.

Against dbt's default `bigquery` driver: dbt `2.0.0-preview.212`, the same default workload, three
alternating rounds; every sample ran the same 39 jobs, no cache hits and the same warehouse results:

| Driver | Median wall | Range | Median jobs/s | Median warehouse span | Median client envelope |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bundled `bigquery` | 24.07s | 23.31–26.26s | 1.62 | 20.50s | 3.57s |
| This driver, as `bigquery_foundry` | 10.74s | 9.50–11.29s | 3.63 | 8.99s | 1.76s |

Paired wall ratio `0.461`; median summed service time 25.00s bundled, 23.91s local. The bundled
driver's per-node timings show client polling latency after short jobs. With 16 threads, this
driver's median wall fell to 8.29s (22.8%, three rounds).

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
with equal responses. These exclude Rust transport, and concurrent lookups' savings do not add to
dbt wall time: five alternating `--existing` pairs, each verifying the jobs, cache hits and
signature, had median walls of 9.82s before and 9.88s after and a paired after/before median of
`0.931` (`0.546`–`1.181`, two regressed), so no end-to-end speedup is established. The live
`table_metadata_describes_what_defines_each_relation` test covers tables, views, materialized views
and external tables.

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
retry, and dbt set no options. `bigquery.get_objects.disable_wildcards=true` removes the second
listing for populated relations; diagnostic builds with and without it each passed 39 jobs with no
cache hits and the expected signature, without establishing a whole-build speedup. An empty
dataset still needs `datasets.get` after `tables.list`, which has no table to report the name
BigQuery resolved the filter to. A catalog filter naming another project, as with a separate
execution project, sends its one-dataset existence probe only when a one-dataset lookup finds
nothing or fails as missing, so a lookup that finds the dataset takes one request instead of two
and a listing one request fewer. Mock tests:

```sh
cargo test --locked --all-features disabling_wildcards_keeps_unfiltered_dataset_and_table_listings
cargo test --locked --all-features a_foreign_catalog_whose_lookups_find_something_is_not_probed
```

[dbt commit `7f6844449`](https://github.com/dbt-labs/dbt/commit/7f6844449b13b920dc9ea4da49806d1d835f718b)
enables literal filters best-effort on dbt `main`; the `2.0.9` binary lacks it. The
[canonical](DBT.md#installing) `0.21.0.dev+dbt0.21.19` and opt-in Foundry `0.21.0.dev+dbt0.1.5`
drivers return `NOT_IMPLEMENTED` for `disable_wildcards` and `skip_table_metadata`.

### REST page size

`jobs.query` and later REST pages request 100,000 rows. In one Rust process against
`example-project` with `bigquery.query.disable_query_cache`, two alternating rounds per build,
reading 600,000 rows of
`SELECT n FROM UNNEST(GENERATE_ARRAY(1, N)) AS n ORDER BY n` over REST alone took 13.57–14.49 s,
against 19.89–20.42 s with no `maxResults` (a first response of up to 350,000 rows) and
10,000-row pages after it. The first response took 3.1–3.2 s holding 100,000 rows and 8.4–8.5 s
holding 350,000: a REST page costs a round trip plus about 20 µs per narrow row. Storage Read's
`jobs.get` and session setup cost about a second before rows arrive.

### Choosing Storage Read for every paginated result

A continuation token on the first completed response selects Storage Read before any rows are
delivered, since a first page's average width cannot rule out a later row over REST's limit.
Complete first responses stay on REST; explicit disabling or an unavailable session keeps REST
fallback.

`example-project` in `US`, `adbc_driver_manager` 1.12.0, PyArrow 26.0.0, default-feature release
`cdylib` builds of a row-count/width threshold (baseline) and the continuation-token rule. Each
connection was warmed by one query, then five pairs ran in one Python process, alternating order,
query cache disabled; timings include execution, fetching every batch and checking counts, sums
and padding lengths. The queries scan no tables.

| Result | Baseline, median (range), s | Continuation-token rule, median (range), s | Median change |
| --- | ---: | ---: | ---: |
| 1,000 narrow rows, complete first response | 0.437 (0.419–0.490) | 0.440 (0.386–0.500) | +0.003 s |
| 120,000 narrow rows, one further REST page | 3.574 (3.415–3.936) | 4.011 (3.694–4.585) | +0.437 s, +12.2% |
| 6,000 rows with 3,000-byte padding, one further REST page | 2.450 (2.351–2.547) | 2.981 (2.788–3.073) | +0.531 s, +21.7% |

Debug diagnostics confirmed that all ten baseline paginated reads used REST and all ten candidate
reads Storage Read; the complete-response control stayed on REST. Small paginated results pay this
setup so that a later wide row cannot fail REST after earlier pages were delivered. The live
regression reads 2,000 rows then one of thirty 800,000-byte strings (24 MB, beyond REST) and checks
every row and string:

```sh
scripts/run-one-live-test.sh reads_a_later_row_too_large_for_rest_through_storage_read
```

### Transaction start

A transaction in a new session sends one `jobs.insert` of `BEGIN TRANSACTION` with
`createSession`, whose RUNNING answer already names the session, instead of a `jobs.query`
`SELECT 1` with `createSession` followed by a named `BEGIN TRANSACTION` job and its
`getQueryResults`. `example-project` with `bigquery.location=US`, release Rust builds, one warmed
database, then five fresh connections per run with autocommit disabled, timing the first
`SELECT 1` through `execute_update`; two alternating runs per build:

| Build | Median | Range |
| --- | ---: | ---: |
| Separate session query | 1.90 s | 1.68–2.37 s |
| Session created by `BEGIN` | 1.24 s | 1.16–1.44 s |

The mock tests in `src/connection/tests/lifecycle.rs` pin the request sequence; check them with
`cargo test --locked --lib connection::tests::lifecycle::`.

### Foundry validation build cache

The [Foundry suite](../integration/foundry-validation/README.md) builds a new wheel each run,
reusing Cargo's `target/` unless `CARGO_TARGET_DIR` overrides it. Linux x86-64, Rust 1.99.1, uv
0.13.5, dependencies cached, live suite running concurrently:

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

In one live run, the unfiltered suite passed 566 tests and 3 subtests with 86 expected skips in
3443.71s of pytest (3452.38s with the wrapper's cached wheel build, datasets and cleanup). The
slowest cases bind streams, since each bound row submits a BigQuery job: INT32 multiple batches
126.08s, INT16 interspersed empty batches 70.90s, INT16 one batch 65.39s.

```sh
PYTEST_ADDOPTS='-q --durations=15' \
  cargo test --locked --test clients foundry::suite_passes -- --ignored
```

### Sequential databases

A closed database's REST connection pool serves the driver's next database, so a process opening a
database per connection, as Python's `dbapi.connect()` does, skips a TCP and TLS handshake and a
reload of the system's root certificates before each first request. Linux x86-64; "one pool per
database" is the same code with `IdlePools::take` (`src/rest/client.rs`) returning `None`:

| Measurement | One pool per database | Pool reuse |
| --- | ---: | ---: |
| 50 databases in turn on one driver, each opening a connection and sending one request to a local keep-alive HTTP/1.1 mock, `instructions:u`, release build | 816 M | 23.8 M |
| Live, ADC user credentials: a later database's first `SELECT 1` minus its second, median over 4 runs of each run's median of 11 databases | 160 ms | 125 ms |

The local count is mostly root-certificate parsing, about 16 M instructions per pool. The remaining
live difference is the token each new database's credential mints, a request that starts when the
connection opens. Reproduce them with:

```sh
cargo test --locked --release --lib --no-run   # prints the unit-test executable
perf stat -e instructions:u <executable> --ignored --exact \
  driver::tests::lifecycle::sequential_databases_measurement
scripts/run-one-live-test.sh sequential_databases_measurement --nocapture
```

### Python package import

`bigquery_adbc.__version__` reads the distribution metadata on first use: `importlib.metadata` and
the email parser it loads cost more to import than the rest of the package. Linux x86-64, Python
3.15.1, the locked `python/` environment, medians of 25 `-X importtime` runs and 7 `perf stat`
runs:

| `import bigquery_adbc` | Eager `__version__` | On first use |
| --- | ---: | ---: |
| Cumulative import time | 27.2 ms | 10.3 ms |
| `instructions:u` beyond `python -c pass` | 211 M | 79 M |

`bigquery_adbc.dbapi` still imports PyArrow, as `adbc_driver_manager.dbapi` does.

```sh
uv sync --project python --locked
python/.venv/bin/python -X importtime -c 'import bigquery_adbc' 2>&1 | tail -n 1
perf stat -e instructions:u python/.venv/bin/python -c 'import bigquery_adbc'
```

### C ABI calls

Every exported call, stream release, runtime wait and result batch checks whether the process is a
`fork` child of the one that created the object. From `AdbcDriverInit` on, a `pthread_atfork`
handler keeps a cached process ID current in place of a `getpid` call, a system call of about
270 ns that was most of the export layer's overhead; Rust callers, which register no handler,
still call `getpid`. The library grew 496 bytes. [`benches/ffi_calls.c`](../benches/ffi_calls.c)
calls the vtable directly with no driver manager or network, 200,000 iterations, three alternating
unpinned runs on a machine shared with other builds; instructions are per iteration, after
subtracting a zero-iteration run:

| Case | `getpid` ns | Cached ns | User instructions | Kernel instructions |
| --- | ---: | ---: | ---: | ---: |
| `StatementSetOption` | 402–406 | 77–82 | 1,615 → 1,613 | 483 → 1 |
| `StatementGetOption` | 367–375 | 59–63 | 1,355 → 1,353 | 483 → 1 |
| `StatementGetOptionDouble` (progress) | 323–326 | 28–31 | 550 → 548 | 483 → 0 |
| `StatementNew` and `StatementRelease` | 1,052–1,121 | 182–205 | 2,125 → 2,118 | 1,447 → 3 |
| `ConnectionGetTableTypes`, read and release | 1,982–2,036 | 494–530 | 7,834 → 7,823 | 2,414 → 5 |

The gain is per call, so it does not show next to BigQuery round trips. Check the handler with
`cargo test --locked --lib a_fork_child_sees_its_own_process_id`; reproduce with:

```sh
cargo rustc --release --locked --lib --crate-type cdylib
cc -O2 benches/ffi_calls.c -o ffi_calls -ldl   # needs <arrow-adbc/adbc.h>
perf stat -e instructions:u,instructions:k \
    ./ffi_calls target/release/libadbc_driver_bigquery.so setopt 200000
```

## Rejected optimizations

Re-evaluating one means rebuilding the rejected code and [measuring again](#reproducing).

| Change | Result |
| --- | --- |
| Column-at-a-time REST conversion | Fewer instructions in `wide/convert/1024` but slower: regrouping per-row cell vectors moves the page through memory again. |
| A flat per-page cell slab instead of a cell vector per row | About one allocation fewer per row, but typed decode and convert slowed slightly and requested bytes rose. |
| Special cases for single-row valid spans and run initialization in nullable dictionary expansion | Weakened the span gather's other gains. |
| Selecting Storage Read by total row count | Rereads rows a complete first response already held; 100,000 narrow rows took longer than staying on REST. |
| At most three queued Storage Write requests | Much slower for 200 batches of 1,024 rows over a residential uplink: the stream waits on round trips. Appends queue up to 30 MB or 1,000 requests; `scripts/run-one-live-test.sh bulk_ingests_many_small_batches --nocapture` prints the elapsed time. |
| A full-sized first Storage Write append | Slower than aiming for 1 MB for one 500,000-row batch: that append is sent alone while a new table may be invisible, and the rest waits on it. |
