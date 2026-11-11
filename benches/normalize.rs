//! Microbenchmark for the Arrow normalization path, over synthetic fixtures: normalization alone,
//! and `encode`, which also sizes each request's window and frames it as Arrow IPC.

mod support;

use adbc_driver_bigquery::benchmark::{NormalizeFixture, NormalizeKind, NormalizeWidth};

use support::{print_header, run_case};

fn main() {
    print_header();
    for rows in [1, 1024, 65_536] {
        for (kind_name, kind) in [
            ("canonical", NormalizeKind::Canonical),
            ("calendar", NormalizeKind::Calendar),
            ("nested", NormalizeKind::Nested),
            ("coercing", NormalizeKind::Coercing),
            ("large-offsets", NormalizeKind::LargeOffsets),
            ("list-gather", NormalizeKind::ListGather),
            ("list-window", NormalizeKind::ListWindow),
            ("decimal-precision", NormalizeKind::DecimalPrecision),
            ("decimal-scale", NormalizeKind::DecimalScale),
            ("dictionary-runs", NormalizeKind::DictionaryRuns),
            ("dictionary-scattered", NormalizeKind::DictionaryScattered),
            (
                "dictionary-alternating",
                NormalizeKind::DictionaryAlternating,
            ),
        ] {
            if rows > 1024
                && !matches!(
                    kind,
                    NormalizeKind::LargeOffsets
                        | NormalizeKind::ListWindow
                        | NormalizeKind::DecimalPrecision
                        | NormalizeKind::DecimalScale
                        | NormalizeKind::DictionaryRuns
                        | NormalizeKind::DictionaryScattered
                        | NormalizeKind::DictionaryAlternating
                )
            {
                continue;
            }
            for (width_name, width) in [
                ("narrow", NormalizeWidth::Narrow),
                ("wide", NormalizeWidth::Wide),
            ] {
                let fixture = NormalizeFixture::new(rows, kind, width);
                run_case(
                    &format!("{kind_name}/{width_name}"),
                    rows,
                    || fixture.prepare_input(),
                    |input| {
                        fixture
                            .normalize(input)
                            .expect("the benchmark fixture must normalize")
                    },
                );
                run_case(
                    &format!("{kind_name}/{width_name}/encode"),
                    rows,
                    || fixture.prepare_input(),
                    |input| {
                        fixture
                            .encode(&input)
                            .expect("the benchmark fixture must encode")
                    },
                );
            }
        }
    }
}
