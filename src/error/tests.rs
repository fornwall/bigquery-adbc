use super::*;
use crate::test_support::errors::{assert_message, assert_message_omits};

const CONTEXT: &str = "BigQuery query execution failed";

/// One entry of the error list a failed job reports.
fn proto(reason: &str, location: &str, message: impl Into<String>) -> ErrorProto {
    ErrorProto {
        reason: reason.to_owned(),
        location: location.to_owned(),
        message: message.into(),
    }
}

/// The failure of a query BigQuery answered without creating a job.
fn failed(errors: &[ErrorProto]) -> Error {
    job_error(CONTEXT, None, errors)
}

fn detail(key: &str, value: &str) -> (String, Vec<u8>) {
    (key.to_owned(), value.as_bytes().to_vec())
}

#[test]
fn job_error_formats_every_entry() {
    let converted = failed(&[
        proto(
            "invalidQuery",
            "query",
            "Unrecognized name: colum; Did you mean column? at [1:8]",
        ),
        proto("", "", "secondary failure"),
    ]);
    assert_eq!(converted.status, Status::InvalidArguments);
    assert_eq!(
        converted.message,
        "BigQuery query execution failed: invalidQuery: Unrecognized name: colum; \
         Did you mean column? at [1:8] (field: query); secondary failure"
    );
}

#[test]
fn job_error_caps_the_service_supplied_error_list() {
    let errors: Vec<ErrorProto> = (0..25)
        .map(|index| {
            let reason = if index == 0 {
                "invalidQuery"
            } else {
                "secondary"
            };
            proto(reason, "", format!("failure {index}"))
        })
        .collect();

    let converted = failed(&errors);
    assert_eq!(converted.status, Status::InvalidArguments);
    assert!(
        converted
            .message
            .starts_with("BigQuery query execution failed: invalidQuery: failure 0; "),
        "{}",
        converted.message
    );
    assert_message(&converted, &["secondary: failure 19"]);
    assert_message_omits(&converted, &["failure 20"]);
    assert!(converted.message.ends_with("; ... and 5 more"));
}

/// `stopped` covers both a requested cancel and a `jobTimeoutMs` expiry, told apart only by the
/// message. Timeout wording under any other reason changes nothing.
#[test]
fn job_error_reads_a_stopped_job_that_timed_out_as_a_timeout() {
    let timed_out = "Job execution was cancelled: Job timed out after 2 sec";
    let cancelled = "Job execution was cancelled: User requested cancellation";
    for (reason, message, expected) in [
        ("stopped", timed_out, Status::Timeout),
        ("stopped", cancelled, Status::Cancelled),
        ("futureReason", timed_out, Status::Unknown),
    ] {
        let converted = failed(&[proto(reason, "", message)]);
        assert_eq!(converted.status, expected, "{reason}: {message}");
    }
}

/// An error's `reason` and `location` are ADBC 1.1.0 details as well as part of the message, and
/// empty ones are left out. Class 42 is "syntax error *or access rule violation*", so a refused
/// query and a denial claim it; `invalid` is any invalid input other than a query, such as a
/// load's bad data, which is no syntax error, and other reasons keep the all-NUL the header asks
/// for.
#[test]
fn structured_fields_and_sqlstate_follow_what_the_service_sent() {
    let class_42 = SQLSTATE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    for (entry, location, sqlstate) in [
        (
            proto("invalidQuery", "query", "no such name"),
            true,
            class_42,
        ),
        (
            proto("accessDenied", "", "Access Denied: p:d.t"),
            false,
            class_42,
        ),
        (proto("rateLimitExceeded", "", ""), false, [0; 5]),
        (
            proto("invalid", "", "Error while reading data"),
            false,
            [0; 5],
        ),
    ] {
        let mut details = vec![detail(DETAIL_REASON, &entry.reason)];
        if location {
            details.push(detail(DETAIL_LOCATION, &entry.location));
        }
        details.push(detail(DETAIL_JOB_VERDICT, "failed"));
        let converted = failed(std::slice::from_ref(&entry));
        assert_eq!(converted.details, Some(details), "{}", entry.reason);
        assert_eq!(converted.sqlstate, sqlstate, "{}", entry.reason);
    }
    let converted = failed(&[]);
    assert_eq!(
        converted.details,
        Some(vec![detail(DETAIL_JOB_VERDICT, "failed")])
    );
    assert_eq!(converted.sqlstate, [0; 5]);
}

/// An entry without a message keeps its `reason:` prefix, which dbt-core 2.0 matches on, and
/// does not claim a job failed: REST rejections render their entries the same way.
#[test]
fn an_entry_without_a_message_keeps_its_reason_prefix() {
    assert_eq!(
        format_error_proto(&proto("notFound", "", "")),
        "notFound: BigQuery gave no message"
    );
}

/// The job name appears in the message and as a detail beside the reason and location.
#[test]
fn a_failed_job_names_itself_in_the_message_and_the_details() {
    let reference = JobReference::new("billing-project", Some("US"), "adbc_job_1".to_owned());
    let errors = [proto("invalidQuery", "query", "Unrecognized name: colum")];

    let converted = job_error(CONTEXT, Some(&reference), &errors);
    assert_eq!(converted.status, Status::InvalidArguments);
    assert_eq!(
        converted.message,
        "BigQuery query execution failed (job billing-project:US.adbc_job_1): invalidQuery: \
         Unrecognized name: colum (field: query) (Query: \
         https://console.cloud.google.com/bigquery?project=billing-project&j=bq:US:adbc_job_1&page=queryresults)"
    );
    assert_eq!(
        converted.details,
        Some(vec![
            detail(DETAIL_REASON, "invalidQuery"),
            detail(DETAIL_LOCATION, "query"),
            detail(DETAIL_JOB_ID, "billing-project:US.adbc_job_1"),
            detail(DETAIL_JOB_VERDICT, "failed"),
        ])
    );
}

/// A job that named no error still exports its identity, the only detail left to debug it by. A
/// query BigQuery answered without creating a job has none to name, and both map like an
/// unrecognized reason.
#[test]
fn a_failure_without_error_entries_keeps_what_identity_it_has() {
    let reference = JobReference::new("billing-project", None, "adbc_job_1".to_owned());
    let converted = job_error(CONTEXT, Some(&reference), &[]);
    assert_eq!(
        converted.details,
        Some(vec![
            detail(DETAIL_JOB_ID, "billing-project:adbc_job_1"),
            detail(DETAIL_JOB_VERDICT, "failed"),
        ])
    );

    let converted = failed(&[]);
    assert_eq!(converted.status, Status::Unknown);
    assert_eq!(
        converted.details,
        Some(vec![detail(DETAIL_JOB_VERDICT, "failed")])
    );
    assert_eq!(
        converted.message,
        "BigQuery query execution failed: the job failed without error details"
    );
}

#[test]
fn a_job_link_preserves_adbc_fields_and_is_only_appended_once() {
    let reference = JobReference::new("billing-project", Some("europe-west1"), "job_1".to_owned());
    let mut source = error("polling failed", Status::IO);
    source.vendor_code = 123;
    source.sqlstate = SQLSTATE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    source.details = Some(vec![("detail".to_owned(), vec![0, 1, 2])]);
    let converted = with_job_link(
        with_job_link(source.clone(), Some(&reference)),
        Some(&reference),
    );
    assert_eq!(
        converted.message,
        "polling failed (Query: https://console.cloud.google.com/bigquery?project=billing-project&j=bq:europe-west1:job_1&page=queryresults)"
    );
    assert_eq!(converted.status, source.status);
    assert_eq!(converted.vendor_code, source.vendor_code);
    assert_eq!(converted.sqlstate, source.sqlstate);
    // The job is exported as a detail too, once, beside the ones the failure already had.
    assert_eq!(
        converted.details,
        Some(vec![
            ("detail".to_owned(), vec![0, 1, 2]),
            detail(DETAIL_JOB_ID, "billing-project:europe-west1.job_1"),
        ])
    );
}

/// A remedy goes after the service's account and before the console link, which stays last.
#[test]
fn a_remedy_is_appended_ahead_of_the_console_link() {
    let mut plain = error(
        "BigQuery rejected the query: stopped: x (HTTP 499)",
        Status::Timeout,
    );
    append_remedy(&mut plain, "raise it");
    assert_eq!(
        plain.message,
        "BigQuery rejected the query: stopped: x (HTTP 499); raise it"
    );

    let reference = JobReference::new("p", Some("EU"), "j".to_owned());
    let mut linked = with_job_link(error("job j failed", Status::Timeout), Some(&reference));
    append_remedy(&mut linked, "raise it");
    assert_eq!(
        linked.message,
        "job j failed; raise it (Query: \
         https://console.cloud.google.com/bigquery?project=p&j=bq:EU:j&page=queryresults)"
    );
}

#[test]
fn job_links_require_a_complete_reference_and_escape_url_components() {
    for reference in [
        None,
        Some(JobReference::new("project", None, "job".to_owned())),
        Some(JobReference::new("", Some("EU"), "job".to_owned())),
        Some(JobReference::new("project", Some("EU"), String::new())),
    ] {
        assert_eq!(
            with_job_link(error("failure", Status::IO), reference.as_ref()).message,
            "failure"
        );
    }
    let reference = JobReference::new("project&other", Some("EU"), "job#1".to_owned());
    assert_eq!(
        with_job_link(error("failure", Status::IO), Some(&reference)).message,
        "failure (Query: https://console.cloud.google.com/bigquery?project=project%26other&j=bq:EU:job%231&page=queryresults)"
    );
}

/// dbt-core 2.0's not-found matcher needs the `notFound:` substring.
#[test]
fn job_error_keeps_the_not_found_spelling_dbt_matches() {
    let not_found = "Not found: Table proj:ds.missing was not found";
    let converted = failed(&[proto("notFound", "", not_found)]);
    assert_eq!(converted.status, Status::NotFound);
    assert_message(&converted, &["notFound:"]);
}
