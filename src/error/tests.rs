use super::*;
use crate::test_support::errors::{assert_message, assert_message_omits};

/// One entry of the error list a failed job reports.
fn proto(reason: &str, location: &str, message: impl Into<String>) -> ErrorProto {
    ErrorProto {
        reason: reason.to_owned(),
        location: location.to_owned(),
        message: message.into(),
    }
}

#[test]
fn job_error_formats_every_entry() {
    let detail = "Unrecognized name: colum; Did you mean column? at [1:8]";
    let errors = vec![
        proto("invalidQuery", "query", detail),
        proto("", "", "secondary failure"),
    ];

    let converted = job_error("BigQuery query execution failed", None, &errors);
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
            let first = index == 0;
            let reason = if first { "invalidQuery" } else { "secondary" };
            proto(reason, "", format!("failure {index}"))
        })
        .collect();

    let converted = job_error("BigQuery query execution failed", None, &errors);
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
    let stopped = job_error(
        "BigQuery query execution failed",
        None,
        &[proto("stopped", "", timed_out)],
    );
    assert_eq!(stopped.status, Status::Timeout);

    let cancelled = job_error(
        "BigQuery query execution failed",
        None,
        &[proto(
            "stopped",
            "",
            "Job execution was cancelled: User requested cancellation",
        )],
    );
    assert_eq!(cancelled.status, Status::Cancelled);

    let unknown = job_error(
        "BigQuery query execution failed",
        None,
        &[proto("futureReason", "", timed_out)],
    );
    assert_eq!(unknown.status, Status::Unknown);
}

/// A job error carries `reason`, `message` and `location` as ADBC 1.1.0 details as well as
/// flattened into the message, and the message keeps the `notFound:` spelling.
#[test]
fn job_error_carries_the_structured_fields_beside_the_message() {
    let errors = vec![proto("invalidQuery", "query", "Unrecognized name: colum")];

    let converted = job_error("BigQuery query execution failed", None, &errors);
    assert_eq!(
        converted.details,
        Some(vec![
            (DETAIL_REASON.to_owned(), b"invalidQuery".to_vec()),
            (DETAIL_LOCATION.to_owned(), b"query".to_vec()),
        ])
    );
    // A refused query is what the standard calls a syntax error or access rule violation.
    assert_eq!(
        converted.sqlstate,
        SQLSTATE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
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

/// Class 42 is "syntax error *or access rule violation*", and a denial is the second half.
#[test]
fn an_access_denial_carries_the_access_rule_violation_sqlstate() {
    let converted = job_error(
        "BigQuery query execution failed",
        None,
        &[proto("accessDenied", "", "Access Denied: Table p:d.t")],
    );
    assert_eq!(converted.status, Status::Unauthorized);
    assert_eq!(
        converted.sqlstate,
        SQLSTATE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

/// Empty fields are left out rather than exported as empty details, and a reason with no
/// SQLSTATE of its own keeps the all-NUL the header asks for.
#[test]
fn structured_fields_are_omitted_when_the_service_did_not_send_them() {
    let converted = job_error(
        "BigQuery query execution failed",
        None,
        &[proto("rateLimitExceeded", "", "")],
    );
    assert_eq!(
        converted.details,
        Some(vec![(
            DETAIL_REASON.to_owned(),
            b"rateLimitExceeded".to_vec()
        )])
    );
    assert_eq!(converted.sqlstate, [0; 5]);

    // `invalid` is BigQuery's reason for invalid input other than a query, such as a load's bad
    // data or a missing field, which is no syntax error.
    let converted = job_error(
        "BigQuery load failed",
        None,
        &[proto("invalid", "", "Error while reading data")],
    );
    assert_eq!(converted.sqlstate, [0; 5]);

    let converted = job_error("BigQuery query execution failed", None, &[]);
    assert_eq!(converted.details, None);
    assert_eq!(converted.sqlstate, [0; 5]);
}

/// The job name appears in the message and as a detail beside the reason and location.
#[test]
fn a_failed_job_names_itself_in_the_message_and_the_details() {
    let reference = JobReference::new("billing-project", Some("US"), "adbc_job_1".to_owned());
    let errors = vec![proto("invalidQuery", "query", "Unrecognized name: colum")];

    let converted = job_error("BigQuery query execution failed", Some(&reference), &errors);
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
            (DETAIL_REASON.to_owned(), b"invalidQuery".to_vec()),
            (DETAIL_LOCATION.to_owned(), b"query".to_vec()),
            (
                DETAIL_JOB_ID.to_owned(),
                b"billing-project:US.adbc_job_1".to_vec()
            ),
        ])
    );
    assert_eq!(
        converted.sqlstate,
        SQLSTATE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

/// A job that named no error still exports its identity, the only detail left to debug it by.
#[test]
fn the_job_id_is_reported_even_when_the_service_named_no_error() {
    let converted = job_error(
        "BigQuery query execution failed",
        Some(&JobReference::new(
            "billing-project",
            None,
            "adbc_job_1".to_owned(),
        )),
        &[],
    );
    assert_eq!(
        converted.details,
        Some(vec![(
            DETAIL_JOB_ID.to_owned(),
            b"billing-project:adbc_job_1".to_vec()
        )])
    );

    // A query BigQuery answered without creating a job has no job to name.
    let converted = job_error("BigQuery query execution failed", None, &[]);
    assert_eq!(converted.details, None);
    assert_eq!(
        converted.message,
        "BigQuery query execution failed: the job failed without error details"
    );
}

/// A job reference is named `project:location.job_id`, and one without a location without it.
#[test]
fn a_job_is_named_the_way_bigquery_names_it() {
    let reference = JobReference::new("billing-project", None, "adbc_job_1".to_owned());
    assert_eq!(
        job_name(&JobReference::new(
            "billing-project",
            Some("US"),
            "adbc_job_1".to_owned()
        )),
        "billing-project:US.adbc_job_1"
    );
    assert_eq!(job_name(&reference), "billing-project:adbc_job_1");
}

#[test]
fn a_job_link_preserves_adbc_fields_and_is_only_appended_once() {
    let reference = JobReference::new("billing-project", Some("europe-west1"), "job_1".to_owned());
    let mut source = error("polling failed", Status::IO);
    source.vendor_code = 123;
    source.sqlstate = [0x34, 0x32, 0x30, 0x30, 0x30];
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
            (
                DETAIL_JOB_ID.to_owned(),
                b"billing-project:europe-west1.job_1".to_vec()
            ),
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

#[test]
fn job_error_keeps_the_not_found_spelling_dbt_matches() {
    let not_found = "Not found: Table proj:ds.missing was not found";
    let errors = vec![proto("notFound", "", not_found)];

    let converted = job_error("BigQuery query execution failed", None, &errors);
    assert_eq!(converted.status, Status::NotFound);
    assert!(
        converted.message.contains("notFound:"),
        "dbt-core 2.0's not-found matcher needs the notFound: substring: {}",
        converted.message
    );
}

/// A job that named no error at all gets the same status as one whose reason is unrecognized.
#[test]
fn job_error_handles_failures_without_details() {
    let converted = job_error("BigQuery query execution failed", None, &[]);
    assert_eq!(converted.status, Status::Unknown);
    assert_eq!(
        converted.message,
        "BigQuery query execution failed: the job failed without error details"
    );
}
