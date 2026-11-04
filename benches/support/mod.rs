#![allow(
    clippy::print_stdout,
    reason = "the benchmark's result table is its user-facing output"
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const SAMPLE_COUNT: usize = 7;
const TARGET_SAMPLE_TIME: Duration = Duration::from_millis(75);
const MAX_ITERATIONS: usize = 100_000;

struct CountingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// SAFETY: every allocation, reallocation and deallocation delegates unchanged to System.
// Tracking uses only non-allocating, non-panicking atomic operations, including wrapping counters.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        // SAFETY: forwards the unchanged allocation request to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwards the pointer and its original layout to the system allocator.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size, Ordering::Relaxed);
        }
        // SAFETY: forwards the pointer, its original layout, and requested size unchanged.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

/// Times one call of `operation`, excluding preparation and the output's drop. `black_box` keeps
/// the optimizer from hoisting the fixture out of the loop or discarding the output.
fn run_once<I, O>(prepare: &impl Fn() -> I, operation: &impl Fn(I) -> O) -> Duration {
    let input = black_box(prepare());
    let started = Instant::now();
    let output = operation(input);
    let elapsed = started.elapsed();
    drop(black_box(output));
    elapsed
}

fn iterations_for<I, O>(prepare: &impl Fn() -> I, operation: &impl Fn(I) -> O) -> usize {
    for _ in 0..3 {
        black_box(run_once(prepare, operation));
    }
    let probe = run_once(prepare, operation).as_nanos().max(1);
    let target = TARGET_SAMPLE_TIME.as_nanos();
    usize::try_from(target / probe)
        .unwrap_or(MAX_ITERATIONS)
        .clamp(1, MAX_ITERATIONS)
}

fn allocation_evidence<I, O>(
    prepare: &impl Fn() -> I,
    operation: &impl Fn(I) -> O,
) -> (usize, usize) {
    // Keep the operation observable so optimization cannot hide its allocations.
    let input = black_box(prepare());
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::Release);
    let output = operation(input);
    TRACK_ALLOCATIONS.store(false, Ordering::Release);
    // Dropped after tracking stops, since dropping a large Arrow tree can allocate.
    drop(black_box(output));
    (
        ALLOCATION_CALLS.load(Ordering::Relaxed),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
    )
}

pub(crate) fn print_header() {
    println!("case                  rows         ns/op         rows/s  alloc calls    alloc bytes");
}

/// How many times `BENCH_COUNT` runs its case.
const COUNTED_ITERATIONS: usize = 1_000;

/// Times one case and prints its row of the result table.
///
/// With `BENCH_COUNT=<case>/<rows>` set, such as `wide/convert/1024`, only that case runs,
/// [`COUNTED_ITERATIONS`] times, without printing timing results. `perf stat -e instructions:u`
/// counts the whole process, including fixture setup, input preparation and timing calls.
pub(crate) fn run_case<I, O>(
    name: &str,
    rows: usize,
    prepare: impl Fn() -> I,
    operation: impl Fn(I) -> O,
) {
    if let Some(selected) = std::env::var_os("BENCH_COUNT") {
        if selected.to_str() == Some(format!("{name}/{rows}").as_str()) {
            for _ in 0..COUNTED_ITERATIONS {
                black_box(run_once(&prepare, &operation));
            }
        }
        return;
    }
    let iterations = iterations_for(&prepare, &operation);
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        let mut elapsed = Duration::ZERO;
        for _ in 0..iterations {
            elapsed += run_once(&prepare, &operation);
        }
        samples.push(elapsed.as_secs_f64() / iterations as f64);
    }
    samples.sort_by(f64::total_cmp);
    let seconds = samples
        .get(SAMPLE_COUNT / 2)
        .copied()
        .expect("the fixed sample count is nonzero");
    let (allocation_calls, allocated_bytes) = allocation_evidence(&prepare, &operation);
    println!(
        "{name:<20} {rows:>5} {ns:>12.0} {throughput:>14.0} {allocation_calls:>12} {allocated_bytes:>14}",
        ns = seconds * 1_000_000_000.0,
        throughput = rows as f64 / seconds,
    );
}
