//! Apache Arrow ADBC's C++ validation suite against the driver's C ABI.
//!
//! The suite is `integration/adbc-validation/`; its README covers coverage and the upstream patch.

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use serde_json::Value;

use crate::datasets::{DatasetLifecycle, OwnedDatasets};
use crate::project_config::project;
use crate::support::{
    REPOSITORY_ROOT, build_driver_library, defer_interrupts, interrupted, progress, run_checked,
    run_client, run_id,
};

const SUITE_DIRECTORY: &str = "integration/adbc-validation";
/// A changed count fails, so that a dependency update requires a coverage review.
const EXPECTED_CASES: usize = 100;
/// The only case that needs a second dataset.
const CROSS_DATASET_CASE: &str = "BigQueryConnectionTest.MetadataGetTableSchemaDbSchema";
/// Skipped by a test-method override in the adapter, so the baseline's hooks cannot record it.
const ADAPTER_SKIPPED_CASE: &str = "BigQueryStatementTest.SqlIngestUInt64";
/// Hooks that weaken an assertion when overridden, each of which must name a recorded case.
const WEAKENING_HOOKS: [&str; 2] = [
    "IngestTemporalValueMultiplier",
    "ingest_null_lists_as_empty",
];
/// Hooks that skip cases by reporting a capability as unavailable.
const SELF_SKIPPING_HOOKS: [&str; 3] = [
    "supports_bulk_ingest_temporary",
    "PrimaryKeyIngestTableDdl",
    "ddl_implicit_commit_txn",
];
/// True skips Transactions and its upstream default of false permits them, unlike other hooks.
const INVERTED_HOOK: &str = "ddl_implicit_commit_txn";

#[derive(Debug, PartialEq)]
enum RecordStatus {
    Relaxed,
    SelfSkipped,
}

/// One `coverage_baseline.txt` line: `<status> <gtest name> <mechanism> <reason>`.
#[derive(Debug)]
struct Record<'a> {
    status: RecordStatus,
    case: &'a str,
    mechanism: &'a str,
}

/// Returns the baseline's records and every line that cannot be one.
fn parse_baseline(baseline: &str) -> (Vec<Record<'_>>, Vec<String>) {
    let mut records: Vec<Record<'_>> = Vec::new();
    let mut errors = Vec::new();
    for line in baseline.lines() {
        let line = line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        let &[status, case, mechanism, _reason, ..] = fields.as_slice() else {
            errors.push(format!(
                "malformed record (want '<status> <test> <mechanism> <reason>'): {line}"
            ));
            continue;
        };
        let status = match status {
            "relaxed" => RecordStatus::Relaxed,
            "self-skipped" => RecordStatus::SelfSkipped,
            _ => {
                errors.push(format!(
                    "{case} has unknown status '{status}'; want relaxed or self-skipped"
                ));
                continue;
            }
        };
        if records.iter().any(|record| record.case == case) {
            errors.push(format!("{case} is recorded twice"));
            continue;
        }
        records.push(Record {
            status,
            case,
            mechanism,
        });
    }
    (records, errors)
}

/// The text after each declaration of `hook` that the adapter marks `override`.
///
/// Declarations wrap across lines, so a declaration ends at the first `;`, `{` or `}`.
fn hook_overrides<'a>(adapter: &'a str, hook: &str) -> impl Iterator<Item = &'a str> {
    adapter.match_indices(hook).filter_map(|(start, name)| {
        let rest = adapter.get(start + name.len()..)?;
        let (declaration, body) = rest.split_at(rest.find([';', '{', '}'])?);
        declaration.contains("override").then_some(body)
    })
}

/// The value returned by a function body that is a single `return` statement.
fn returned_value(body: &str) -> Option<&str> {
    let statement = body
        .strip_prefix('{')?
        .trim_start()
        .strip_prefix("return ")?;
    let (value, rest) = statement.split_once(';')?;
    rest.trim_start().starts_with('}').then_some(value.trim())
}

/// Whether the adapter reports a capability as unavailable, which makes a case self-skip: the hook
/// keeps its upstream default or returns the unavailable value outright.
fn reports_unsupported(adapter: &str, hook: &str) -> bool {
    let inverted = hook == INVERTED_HOOK;
    let mut overrides = hook_overrides(adapter, hook).peekable();
    if overrides.peek().is_none() {
        return !inverted;
    }
    let unavailable: &[&str] = if inverted {
        &["true"]
    } else {
        &["false", "std::nullopt"]
    };
    overrides.any(|body| returned_value(body).is_some_and(|value| unavailable.contains(&value)))
}

/// Checks the baseline against the adapter in both directions: a record cannot outlive its
/// mechanism, and a mechanism cannot exist without a record.
fn adapter_drift(records: &[Record<'_>], adapter: &str) -> Vec<String> {
    let mut errors = Vec::new();
    for record in records {
        let Record {
            status,
            case,
            mechanism,
        } = record;
        match status {
            RecordStatus::Relaxed if hook_overrides(adapter, mechanism).next().is_none() => {
                errors.push(format!(
                    "{case} is recorded as relaxed through {mechanism}, but bigquery_validation.cc \
                     no longer overrides it; delete the record if the relaxation is gone"
                ));
            }
            RecordStatus::SelfSkipped if !reports_unsupported(adapter, mechanism) => {
                errors.push(format!(
                    "{case} is recorded as self-skipped, but bigquery_validation.cc no longer \
                     reports {mechanism} as unavailable, so the case runs; move it out of the \
                     record"
                ));
            }
            RecordStatus::Relaxed | RecordStatus::SelfSkipped => {}
        }
    }
    let records_hook = |status: RecordStatus, hook: &str| {
        records
            .iter()
            .any(|record| record.status == status && record.mechanism == hook)
    };
    for hook in WEAKENING_HOOKS {
        if hook_overrides(adapter, hook).next().is_some()
            && !records_hook(RecordStatus::Relaxed, hook)
        {
            errors.push(format!(
                "bigquery_validation.cc overrides {hook} but coverage_baseline.txt records no \
                 case relaxed by it"
            ));
        }
    }
    for hook in SELF_SKIPPING_HOOKS {
        if reports_unsupported(adapter, hook) && !records_hook(RecordStatus::SelfSkipped, hook) {
            errors.push(format!(
                "bigquery_validation.cc reports {hook} as unavailable but coverage_baseline.txt \
                 records no case skipped by it"
            ));
        }
    }
    errors
}

/// Parses the baseline and checks it against the adapter source.
fn baseline_drift<'a>(baseline: &'a str, adapter: &str) -> (Vec<Record<'a>>, Vec<String>) {
    let (records, mut errors) = parse_baseline(baseline);
    errors.extend(adapter_drift(&records, adapter));
    (records, errors)
}

fn suite_file(name: &str) -> String {
    fs::read_to_string(Path::new(REPOSITORY_ROOT).join(SUITE_DIRECTORY).join(name)).unwrap()
}

#[track_caller]
fn assert_no_drift(errors: &[String]) {
    assert!(
        errors.is_empty(),
        "coverage baseline drifted from the suite; see {SUITE_DIRECTORY}/coverage_baseline.txt:\n   {}",
        errors.join("\n   ")
    );
}

/// The built suite, enumerated and checked against the coverage baseline.
#[derive(Debug)]
struct Harness {
    binary: PathBuf,
    library: PathBuf,
    build_directory: PathBuf,
    cases: Vec<String>,
    expected_skips: BTreeSet<String>,
}

/// Builds the suite once: its tests run concurrently and share one CMake build directory.
fn harness() -> &'static Harness {
    static HARNESS: OnceLock<Harness> = OnceLock::new();
    HARNESS.get_or_init(build_harness)
}

fn build_harness() -> Harness {
    project();
    let sanitize = std::env::var("ADBC_VALIDATION_SANITIZE").unwrap_or_default();
    let build_directory = Path::new(REPOSITORY_ROOT).join(if sanitize.is_empty() {
        ".adbc-validation-build"
    } else {
        ".adbc-validation-build-san"
    });

    progress("building Rust BigQuery cdylib");
    let library = build_driver_library(&[]);

    progress("configuring C++ ADBC validation harness");
    run_checked(
        Command::new("cmake")
            .arg("-UFETCHCONTENT_SOURCE_DIR_ARROW_ADBC")
            .arg("-S")
            .arg(Path::new(REPOSITORY_ROOT).join(SUITE_DIRECTORY))
            .arg("-B")
            .arg(&build_directory)
            .arg("-DCMAKE_BUILD_TYPE=Debug")
            .arg("-DCMAKE_POLICY_VERSION_MINIMUM=3.5")
            .arg(format!("-DBIGQUERY_VALIDATION_SANITIZE={sanitize}")),
    );
    run_checked(
        Command::new("cmake")
            .arg("--build")
            .arg(&build_directory)
            .args([
                "--target",
                "bigquery_validation",
                "query_example",
                "--parallel",
            ]),
    );

    let binary = build_directory.join("bigquery_validation");
    let listing = Command::new(&binary)
        .arg("--gtest_list_tests")
        .env_remove("GTEST_FILTER")
        .output()
        .unwrap();
    assert!(listing.status.success(), "failed to list validation cases");
    let cases = listed_cases(&String::from_utf8(listing.stdout).unwrap());
    assert_eq!(
        cases.len(),
        EXPECTED_CASES,
        "expected {EXPECTED_CASES} canonical validation cases"
    );
    progress(format!(
        "test count OK: {} canonical validation cases",
        cases.len()
    ));

    let baseline = suite_file("coverage_baseline.txt");
    let (records, mut errors) = baseline_drift(&baseline, &suite_file("bigquery_validation.cc"));
    for record in &records {
        if !cases.iter().any(|case| case == record.case) {
            let case = record.case;
            errors.push(format!(
                "{case} is recorded but no longer exists in the suite"
            ));
        }
    }
    assert_no_drift(&errors);
    let self_skipped: Vec<&str> = records
        .iter()
        .filter(|record| record.status == RecordStatus::SelfSkipped)
        .map(|record| record.case)
        .collect();
    progress(format!(
        "coverage baseline OK: {} relaxed, {} self-skipped of {} cases",
        records.len() - self_skipped.len(),
        self_skipped.len(),
        cases.len(),
    ));

    let expected_skips = self_skipped
        .into_iter()
        .chain([ADAPTER_SKIPPED_CASE])
        .map(str::to_owned)
        .collect();
    Harness {
        binary,
        library,
        build_directory,
        cases,
        expected_skips,
    }
}

/// Reads `--gtest_list_tests` output: unindented `Suite.` lines, each followed by indented cases.
fn listed_cases(listing: &str) -> Vec<String> {
    let mut suite = "";
    let mut cases = Vec::new();
    for line in listing.lines() {
        let Some(name) = line.split_whitespace().next() else {
            continue;
        };
        if line.starts_with(char::is_whitespace) {
            cases.push(format!("{suite}{name}"));
        } else {
            suite = name;
        }
    }
    cases
}

impl Harness {
    /// Goes through the validation binary, which checks the dataset's ownership prefix. Its own
    /// process group keeps a Ctrl-C at the terminal from abandoning a create or drop halfway.
    fn dataset_command(&self, action: &str, dataset: &str) -> Result<(), String> {
        progress(format!("{action} BigQuery validation dataset {dataset}"));
        let status = Command::new(&self.binary)
            .arg(format!("--adbc-{action}-dataset"))
            .env("ADBC_BIGQUERY_LIBRARY", &self.library)
            .env("ADBC_BIGQUERY_DATASET", dataset)
            .process_group(0)
            .status()
            .map_err(|error| format!("failed to {action} {dataset}: {error}"))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("failed to {action} {dataset}: {status}"))
        }
    }
}

impl DatasetLifecycle for &Harness {
    fn create(&self, dataset: &str) -> Result<(), String> {
        self.dataset_command("create", dataset)
    }

    fn drop_dataset(&self, dataset: &str) -> Result<(), String> {
        self.dataset_command("drop", dataset)
    }
}

#[derive(Debug, PartialEq)]
enum CaseOutcome {
    Passed,
    Skipped,
}

/// Reads one case's result from GoogleTest's JSON report. GoogleTest exits 0 for a skip, so only
/// the report tells a skip from a pass, and a case the filter failed to select must not count.
fn case_outcome(report: &str, case: &str) -> Result<CaseOutcome, String> {
    let report: Value = serde_json::from_str(report).map_err(|error| error.to_string())?;
    let (suite_name, case_name) = case
        .split_once('.')
        .ok_or_else(|| format!("{case} has no suite name"))?;
    let results: Vec<&Value> = report["testsuites"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|suite| suite["name"] == suite_name)
        .filter_map(|suite| suite["testsuite"].as_array())
        .flatten()
        .filter(|result| result["name"] == case_name)
        .collect();
    let &[result] = results.as_slice() else {
        return Err(format!("report has {} results for the case", results.len()));
    };
    if result.get("failures").is_some() {
        return Err("report records a failure, but the process exited 0".to_owned());
    }
    match (result["status"].as_str(), result["result"].as_str()) {
        (Some("RUN"), Some("COMPLETED")) => Ok(CaseOutcome::Passed),
        (Some("RUN"), Some("SKIPPED")) => Ok(CaseOutcome::Skipped),
        (status, outcome) => Err(format!("unexpected status {status:?}, result {outcome:?}")),
    }
}

/// Holds a finished case to the baseline: a skip nobody recorded hides lost coverage, and a pass
/// despite a record means the record no longer describes the suite.
fn check_outcome(outcome: &CaseOutcome, expected_skip: bool) -> Result<(), String> {
    match (outcome, expected_skip) {
        (CaseOutcome::Passed, false) | (CaseOutcome::Skipped, true) => Ok(()),
        (CaseOutcome::Skipped, false) => {
            Err("skipped, but coverage_baseline.txt records no skip for it".to_owned())
        }
        (CaseOutcome::Passed, true) => {
            Err("passed, but coverage_baseline.txt records it as self-skipped".to_owned())
        }
    }
}

/// Runs one case in `primary`, recording its datasets in `datasets` for the caller to drop.
fn run_case(
    harness: &Harness,
    case: &str,
    primary: &str,
    datasets: &mut OwnedDatasets<&Harness>,
) -> Result<CaseOutcome, String> {
    let secondary = format!("{primary}_secondary");
    datasets.create(primary)?;
    if case == CROSS_DATASET_CASE {
        datasets.create(&secondary)?;
    }

    progress(format!("running {case}"));
    let report_path = harness.build_directory.join(format!("{primary}.json"));
    let status = run_client(
        Command::new(&harness.binary)
            .arg("--gtest_brief=1")
            .arg(format!("--gtest_filter={case}"))
            .arg(format!("--gtest_output=json:{}", report_path.display()))
            .env("ADBC_BIGQUERY_LIBRARY", &harness.library)
            .env("ADBC_BIGQUERY_DATASET", primary)
            .env("ADBC_BIGQUERY_SECONDARY_DATASET", &secondary),
    )
    .map_err(|error| format!("failed to run: {error}"))?;
    let report = fs::read_to_string(&report_path);
    // A missing report is judged below, and a leftover one is only a file in the build directory.
    let _ = fs::remove_file(&report_path);
    if !status.success() {
        return Err(format!("exited with {status}"));
    }
    let report = report.map_err(|error| format!("no report: {error}"))?;
    let outcome = case_outcome(&report, case)?;
    check_outcome(&outcome, harness.expected_skips.contains(case))?;
    Ok(outcome)
}

/// Fails every recorded relaxation or skip that the adapter no longer backs, without building.
#[test]
fn coverage_baseline_matches_adapter() {
    let baseline = suite_file("coverage_baseline.txt");
    let (records, errors) = baseline_drift(&baseline, &suite_file("bigquery_validation.cc"));
    assert_no_drift(&errors);
    assert!(!records.is_empty());
}

#[test]
#[ignore = "builds the pinned C++ suite, which needs CMake and a C++20 compiler"]
fn suite_matches_coverage_baseline() {
    harness();
}

/// Each case gets fresh datasets, so a case's leftovers cannot affect another. Failures accumulate
/// and are listed at the end; a dataset that cannot be dropped stops the run instead.
#[test]
#[ignore = "creates and drops BigQuery datasets and needs Application Default Credentials"]
fn suite_passes_against_bigquery() {
    let harness = harness();
    let run_prefix = format!("bigquery_adbc_validation_{}", run_id());
    defer_interrupts();

    let mut skipped = 0;
    let mut failures = Vec::new();
    for (index, case) in harness.cases.iter().enumerate() {
        assert!(!interrupted(), "interrupted before {case}");
        let primary = format!("{run_prefix}_{:03}", index + 1);
        let mut datasets = OwnedDatasets::new(harness);
        match run_case(harness, case, &primary, &mut datasets) {
            Ok(CaseOutcome::Passed) => {}
            Ok(CaseOutcome::Skipped) => skipped += 1,
            Err(error) => {
                progress(format!("FAILED {case}: {error}"));
                failures.push(format!("{case}: {error}"));
            }
        }
        if let Err(error) = datasets.release() {
            panic!("stopping after {case}: {error}");
        }
    }
    let total = harness.cases.len();
    let passed = total - skipped - failures.len();
    progress(format!(
        "{passed} passed, {skipped} skipped of {total} cases"
    ));
    assert!(
        failures.is_empty(),
        "{} case(s) failed:\n   {}",
        failures.len(),
        failures.join("\n   ")
    );
}

#[cfg(test)]
mod false_success {
    use super::*;

    const ADAPTER: &str = "
        bool ddl_implicit_commit_txn() const override { return true; }
        std::optional<std::string> PrimaryKeyIngestTableDdl(
            std::string_view) const override {
          return std::nullopt;
        }
        bool ingest_null_lists_as_empty() const override { return true; }
    ";
    const BASELINE: &str = "
        # comment
        relaxed       A.Lists      ingest_null_lists_as_empty     null compared as empty
        self-skipped  A.Temporary  supports_bulk_ingest_temporary no temporary tables
        self-skipped  A.Key        PrimaryKeyIngestTableDdl       no primary key
        self-skipped  A.Rollback   ddl_implicit_commit_txn        no DDL rollback
    ";

    fn drift(baseline: &str, adapter: &str) -> Vec<String> {
        baseline_drift(baseline, adapter).1
    }

    #[test]
    fn a_consistent_baseline_has_no_drift() {
        assert_eq!(drift(BASELINE, ADAPTER), Vec::<String>::new());
    }

    #[test]
    fn a_record_cannot_outlive_its_mechanism() {
        let relaxation_removed = ADAPTER.replace("ingest_null_lists_as_empty", "unrelated");
        assert!(drift(BASELINE, &relaxation_removed)[0].contains("no longer overrides"));

        let capability_added = ADAPTER.replace("return std::nullopt", "return \"DDL\"");
        assert!(drift(BASELINE, &capability_added)[0].contains("no longer reports"));

        let transactions_enabled = ADAPTER.replace(
            "return true; }\n        std",
            "return false; }\n        std",
        );
        assert!(drift(BASELINE, &transactions_enabled)[0].contains("ddl_implicit_commit_txn"));
    }

    #[test]
    fn a_mechanism_cannot_exist_without_a_record() {
        let weakening = BASELINE.replace("relaxed       A.Lists", "# relaxed");
        assert!(drift(&weakening, ADAPTER)[0].contains("no case relaxed"));

        let skip = BASELINE.replace("self-skipped  A.Key", "# self-skipped");
        assert!(drift(&skip, ADAPTER)[0].contains("no case skipped"));
    }

    #[test]
    fn malformed_baseline_lines_are_errors() {
        for line in [
            "relaxed A.Case ingest_null_lists_as_empty",
            "ignored A.Case ingest_null_lists_as_empty reason",
        ] {
            assert_eq!(parse_baseline(line).1.len(), 1, "{line}");
        }
        let repeated = "relaxed A.Case hook reason\nrelaxed A.Case hook reason";
        assert_eq!(parse_baseline(repeated).1.len(), 1);
    }

    #[test]
    fn listing_prefixes_cases_with_their_suite() {
        let listing = "A.\n  One\n  Two  # GetParam() = 1\n\nB.\n  Three\n";
        assert_eq!(listed_cases(listing), ["A.One", "A.Two", "B.Three"]);
    }

    fn report(result: &str) -> String {
        format!(r#"{{"testsuites":[{{"name":"A","testsuite":[{result}]}}]}}"#)
    }

    #[test]
    fn only_a_report_of_the_selected_case_counts() {
        let passed = r#"{"name":"One","status":"RUN","result":"COMPLETED"}"#;
        let skipped = r#"{"name":"One","status":"RUN","result":"SKIPPED"}"#;
        assert_eq!(
            case_outcome(&report(passed), "A.One"),
            Ok(CaseOutcome::Passed)
        );
        assert_eq!(
            case_outcome(&report(skipped), "A.One"),
            Ok(CaseOutcome::Skipped)
        );

        let unselected = r#"{"name":"One","status":"NOTRUN","result":"SUPPRESSED"}"#;
        let failed = r#"{"name":"One","status":"RUN","result":"COMPLETED","failures":[{}]}"#;
        assert!(case_outcome(&report(unselected), "A.One").is_err());
        assert!(case_outcome(&report(failed), "A.One").is_err());
        assert!(case_outcome(&report(passed), "A.Two").is_err());
        assert!(case_outcome(&report(&format!("{passed},{passed}")), "A.One").is_err());
        assert!(case_outcome("{}", "A.One").is_err());
        assert!(case_outcome("not json", "A.One").is_err());
    }

    #[test]
    fn an_outcome_must_agree_with_the_baseline() {
        assert!(check_outcome(&CaseOutcome::Passed, false).is_ok());
        assert!(check_outcome(&CaseOutcome::Skipped, true).is_ok());
        assert!(check_outcome(&CaseOutcome::Skipped, false).is_err());
        assert!(check_outcome(&CaseOutcome::Passed, true).is_err());
    }
}
