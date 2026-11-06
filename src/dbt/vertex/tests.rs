use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use adbc_core::error::Status;
use adbc_core::{Connection as _, Statement as _};
use serde_json::json;

use super::job_outcome;
use crate::BigQueryStatementOption as Key;
use crate::rest::url::escape_resource_path as escape;
use crate::test_support::credentials::TEST_PRIVATE_KEY;
use crate::test_support::driver::{test_connection_with, test_statement};
use crate::test_support::errors::assert_message;
use crate::test_support::http::{capture_requests, capture_requests_gated, error_body};
use crate::test_support::options::{assert_string_option, set_option};
use crate::{BigQueryConnection, BigQueryDatabaseOption, BigQueryStatement};

const PARENT: &str = "projects/data-project/locations/us-central1";
const TEMPLATE: &str = "projects/data-project/locations/us-central1/notebookRuntimeTemplates/1234";
const JOB: &str = "projects/data-project/locations/us-central1/notebookExecutionJobs/nej-1";
const LRO: &str = "projects/data-project/locations/us-central1/notebookExecutionJobs/nej-1\
                   /operations/op-1";

/// A test connection whose Vertex AI calls go to `endpoint`. BigQuery points at a closed
/// port, so a stray BigQuery request fails rather than reaching the mock.
fn connection_running_notebooks_at(endpoint: String) -> BigQueryConnection {
    test_connection_with(
        "http://127.0.0.1:1",
        [(
            BigQueryDatabaseOption::VertexEndpoint.into(),
            endpoint.into(),
        )],
    )
}

const SERVICE_ACCOUNT: &str = "dbt-runner@data-project.iam.gserviceaccount.com";

fn service_account_key() -> String {
    json!({
        "type": "service_account",
        "project_id": "data-project",
        "private_key_id": "0123456789abcdef",
        "private_key": TEST_PRIVATE_KEY,
        "client_email": SERVICE_ACCOUNT,
    })
    .to_string()
}

/// The mock is plaintext loopback, which a credential-bearing configuration refuses unless
/// `bigquery.allow_untrusted_endpoint` is set.
fn connection_running_notebooks_as_a_service_account(endpoint: String) -> BigQueryConnection {
    test_connection_with(
        "http://127.0.0.1:1",
        [
            (
                BigQueryDatabaseOption::VertexEndpoint.into(),
                endpoint.into(),
            ),
            (
                BigQueryDatabaseOption::AuthenticationType.into(),
                crate::BigQueryAuthenticationType::JsonCredentialString.into(),
            ),
            (
                BigQueryDatabaseOption::AuthenticationCredentials.into(),
                service_account_key().into(),
            ),
            (
                BigQueryDatabaseOption::AllowUntrustedEndpoint.into(),
                "true".into(),
            ),
        ],
    )
}

/// The `bigframes` option order and SQL placeholder emitted by dbt-core 2.0.
fn bigframes_statement(
    connection: &mut BigQueryConnection,
    template_id: Option<&str>,
    pooling_timeout: &str,
) -> BigQueryStatement {
    let mut statement = connection.new_statement().unwrap();
    for (option, value) in [
        (
            Key::NotebookExecuteJobGscPath,
            "gs://dbt-python/analytics/my_model.py",
        ),
        (
            Key::NotebookExecuteJobModelFileName,
            "analytics/my_model.py",
        ),
        (Key::NotebookExecuteJobModelName, "my_model"),
        (Key::NotebookExecuteJobGscBucket, "dbt-python"),
        (Key::NotebookExecuteJobParent, PARENT),
        (Key::NotebookExecuteJobProject, "data-project"),
        (Key::NotebookExecuteJobRegion, "us-central1"),
        (Key::CreateBatchBatchId, "5e2a7c18-batch"),
        (Key::DataprocPoolingTimeout, pooling_timeout),
    ] {
        set_option(&mut statement, option, value.to_owned());
    }
    if let Some(template_id) = template_id {
        set_option(
            &mut statement,
            Key::NotebookExecuteJobTemplateId,
            template_id.to_owned(),
        );
    }
    // dbt-core 2.0 finalizes the statement with a real, executable query, which must not run.
    statement.set_sql_query("SELECT 1").unwrap();
    statement
}

fn lro(done: bool) -> serde_json::Value {
    json!({"name": LRO, "done": done})
}

fn job_in_state(state: &str) -> serde_json::Value {
    json!({"name": JOB, "jobState": state})
}

#[test]
fn dbt_bigframes_model_submits_one_notebook_execution_job() {
    let (endpoint, captured) = capture_requests(vec![
        (
            200,
            json!({"notebookRuntimeTemplates": [{"name": TEMPLATE}]}),
        ),
        (200, lro(true)),
        (200, job_in_state("JOB_STATE_RUNNING")),
        (200, job_in_state("JOB_STATE_SUCCEEDED")),
    ]);
    let mut connection = connection_running_notebooks_at(endpoint);
    let mut statement = bigframes_statement(&mut connection, None, "3600");

    let (mut reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(
        rows_affected, -1,
        "a notebook run affects no rows, which ADBC spells as unknown rather than zero"
    );
    assert_eq!(reader.schema().fields().len(), 0);
    assert_eq!(
        reader
            .schema()
            .metadata()
            .get(crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID),
        Some(&JOB.to_owned()),
        "the job is the only place the notebook's output can be found, and dbt-core 2.0 copies \
         this key into run_results.json"
    );
    assert!(reader.next().is_none());

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 4, "list, create, and two job polls");
    assert_eq!(
        requests[0].path(),
        "/v1/projects/data-project/locations/us-central1/notebookRuntimeTemplates\
         ?filter=notebookRuntimeType%20%3D%20ONE_CLICK",
        "the ONE_CLICK filter reaches Vertex AI percent-encoded"
    );
    assert!(
        requests[1].line.starts_with(
            "POST /v1/projects/data-project/locations/us-central1/notebookExecutionJobs "
        ),
        "{}",
        requests[1].line
    );
    assert_eq!(
        requests[1].body,
        json!({
            "gcsNotebookSource": {"uri": "gs://dbt-python/analytics/my_model.py"},
            "gcsOutputUri": "gs://dbt-python/analytics/my_model.py/logs",
            "displayName": "5e2a7c18-batch",
            "notebookRuntimeTemplateResourceName": TEMPLATE,
        }),
        "the display name comes from the Dataproc-namespaced create_batch.batch_id, and the \
         output URI from the misspelled gsc_bucket plus model_file_name"
    );
    assert!(
        requests[1].body.get("serviceAccount").is_none()
            && requests[1].body.get("executionUser").is_none(),
        "credentials that name no principal send no execution identity at all, rather than an \
         empty half of the oneof: {}",
        requests[1].body
    );
    for poll in &requests[2..] {
        assert!(
            poll.line.starts_with(&format!("GET /v1/{JOB} ")),
            "{}",
            poll.line
        );
    }
}

/// dbt-core 2.0 sends the template ID only when `notebook_template_id` parses as an integer.
#[test]
fn a_configured_template_id_names_the_template_without_listing() {
    let (endpoint, captured) = capture_requests(vec![
        (200, lro(true)),
        (200, job_in_state("JOB_STATE_SUCCEEDED")),
    ]);
    let mut connection = connection_running_notebooks_at(endpoint);
    let mut statement = bigframes_statement(&mut connection, Some("4242"), "3600");

    assert_eq!(statement.execute_update().unwrap(), None);

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 2, "no runtime template listing");
    assert_eq!(
        requests[0].body["notebookRuntimeTemplateResourceName"],
        json!("projects/data-project/locations/us-central1/notebookRuntimeTemplates/4242"),
        "a bare template ID is qualified with the notebook project and region"
    );
}

#[test]
fn a_refused_request_names_the_resource_it_addressed() {
    let denied = json!({"error": {"code": 403, "message": "Vertex AI API has not been used"}});
    let (endpoint, captured) = capture_requests(vec![
        (403, denied.clone()),
        (403, denied.clone()),
        (200, lro(true)),
        (403, denied),
    ]);
    let mut connection = connection_running_notebooks_at(endpoint);

    let cases = [
        (
            None,
            format!("failed to list the BigQuery Vertex AI notebook runtime templates in {PARENT}"),
        ),
        (
            Some("4242"),
            format!(
                "failed to create the BigQuery Vertex AI notebook execution job 5e2a7c18-batch in \
                 {PARENT}"
            ),
        ),
        (
            Some("4242"),
            format!("failed to read the BigQuery Vertex AI notebook execution job {JOB}"),
        ),
    ];
    for (template_id, expected) in cases {
        let error = bigframes_statement(&mut connection, template_id, "0")
            .execute_update()
            .unwrap_err();
        assert_eq!(error.status, Status::Unauthorized, "{}", error.message);
        assert_message(
            &error,
            &[&format!("{expected}: Vertex AI API has not been used")],
        );
    }
    assert_eq!(captured.join().unwrap().len(), 4);
}

/// The template listing is a read, so a transient failure is resent. The create carries no
/// request ID that would let Vertex AI deduplicate it, so it is sent once.
#[test]
fn only_the_template_listing_is_resent_after_a_transient_failure() {
    let unavailable = error_body("backendError", "The service is unavailable.");
    let (endpoint, captured) = capture_requests(vec![
        (503, unavailable.clone()),
        (
            200,
            json!({"notebookRuntimeTemplates": [{"name": TEMPLATE}]}),
        ),
        (503, unavailable),
    ]);
    let mut connection = connection_running_notebooks_at(endpoint);

    let error = bigframes_statement(&mut connection, None, "0")
        .execute_update()
        .unwrap_err();
    assert_eq!(error.status, Status::IO, "{}", error.message);
    assert_message(
        &error,
        &["failed to create the BigQuery Vertex AI notebook execution job"],
    );
    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 3, "two listings and one create");
    assert_eq!(requests[0].line, requests[1].line);
    assert!(
        requests[2].line.starts_with("POST "),
        "{}",
        requests[2].line
    );
}

#[test]
fn a_template_id_that_reshapes_the_resource_path_is_refused() {
    // An empty value clears the option rather than naming a template, so it is not tested here.
    for template_id in [
        "../../other-project/locations/us/notebookRuntimeTemplates/1",
        "a/b",
        ".",
    ] {
        let (endpoint, captured) = capture_requests(Vec::new());
        let mut connection = connection_running_notebooks_at(endpoint);
        let mut statement = bigframes_statement(&mut connection, Some(template_id), "3600");

        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_message(&error, &["adbc.bigquery.notebook_execute_job.template_id"]);
        assert!(
            captured.join().unwrap().is_empty(),
            "nothing is sent for a template ID that was never a resource name"
        );
    }
}

/// Where the fork answers an empty listing by creating a durable `default-one-click-notebook`
/// template, this reports the missing template.
#[test]
fn a_project_without_a_one_click_template_is_not_given_one() {
    let (endpoint, captured) =
        capture_requests(vec![(200, json!({"notebookRuntimeTemplates": []}))]);
    let mut connection = connection_running_notebooks_at(endpoint);
    let mut statement = bigframes_statement(&mut connection, None, "3600");

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert_message(
        &error,
        &[PARENT, "adbc.bigquery.notebook_execute_job.template_id"],
    );
    assert_eq!(
        captured.join().unwrap().len(),
        1,
        "nothing is created on the caller's behalf"
    );
}

#[test]
fn template_lookup_continues_past_empty_pages() {
    let (endpoint, captured) = capture_requests(vec![
        (200, json!({"nextPageToken": "first+/=&"})),
        (
            200,
            json!({"notebookRuntimeTemplates": [], "nextPageToken": "second"}),
        ),
        (
            200,
            json!({"notebookRuntimeTemplates": [{"name": TEMPLATE}]}),
        ),
        (200, lro(true)),
        (200, job_in_state("JOB_STATE_SUCCEEDED")),
    ]);
    let mut connection = connection_running_notebooks_at(endpoint);
    bigframes_statement(&mut connection, None, "3600")
        .execute_update()
        .unwrap();

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 5);
    let first = requests[0].path();
    assert_eq!(
        requests[1].path(),
        format!("{first}&pageToken=first%2B%2F%3D%26")
    );
    assert_eq!(requests[2].path(), format!("{first}&pageToken=second"));
    assert_eq!(
        requests[3].body["notebookRuntimeTemplateResourceName"],
        TEMPLATE
    );
}

#[test]
fn template_lookup_reports_not_found_only_after_the_last_page() {
    let (endpoint, captured) = capture_requests(vec![
        (200, json!({"nextPageToken": "next"})),
        (200, json!({"nextPageToken": ""})),
    ]);
    let mut connection = connection_running_notebooks_at(endpoint);
    let failure = bigframes_statement(&mut connection, None, "3600")
        .execute_update()
        .unwrap_err();
    assert_eq!(failure.status, Status::NotFound);
    assert_eq!(captured.join().unwrap().len(), 2);
}

#[test]
fn template_lookup_rejects_pagination_cycles_without_a_timeout() {
    for tokens in [vec!["a", "a"], vec!["a", "b", "a"]] {
        let (endpoint, captured) = capture_requests(
            tokens
                .iter()
                .map(|token| (200, json!({"nextPageToken": token})))
                .collect(),
        );
        let mut connection = connection_running_notebooks_at(endpoint);
        let failure = bigframes_statement(&mut connection, None, "0")
            .execute_update()
            .unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_message(&failure, &["repeated a page token"]);
        assert_eq!(captured.join().unwrap().len(), tokens.len());
    }
}

#[test]
fn malformed_template_listings_are_invalid_data() {
    for listed in [
        json!({"notebookRuntimeTemplates": {}}),
        json!({"notebookRuntimeTemplates": [{}]}),
        json!({"notebookRuntimeTemplates": [{"name": ""}]}),
        json!({"nextPageToken": 1}),
    ] {
        let (endpoint, captured) = capture_requests(vec![(200, listed)]);
        let mut connection = connection_running_notebooks_at(endpoint);
        let failure = bigframes_statement(&mut connection, None, "3600")
            .execute_update()
            .unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(captured.join().unwrap().len(), 1);
    }
}

#[test]
fn failed_notebook_jobs_preserve_the_service_status_and_message() {
    for (state, code, message, status) in [
        (
            "JOB_STATE_FAILED",
            7,
            "the Colab service agent cannot read gs://dbt-python",
            Status::Unauthorized,
        ),
        (
            "JOB_STATE_PARTIALLY_SUCCEEDED",
            3,
            "cell 2 raised ValueError",
            Status::InvalidArguments,
        ),
    ] {
        let (endpoint, captured) = capture_requests(vec![
            (200, lro(true)),
            (
                200,
                json!({"name": JOB, "jobState": state,
                "status": {"code": code, "message": message}}),
            ),
        ]);
        let mut connection = connection_running_notebooks_at(endpoint);
        let mut statement = bigframes_statement(&mut connection, Some("4242"), "3600");

        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, status, "{state}");
        assert_message(&error, &[JOB, state, message]);
        assert_eq!(captured.join().unwrap().len(), 2);
    }
}

#[test]
fn a_failed_poll_says_the_notebook_job_may_still_be_running() {
    let denied = json!({"error": {"code": 403, "message": "Permission denied"}});
    for responses in [
        vec![(200, lro(false)), (403, denied.clone())],
        vec![(200, lro(true)), (403, denied)],
    ] {
        let (endpoint, captured) = capture_requests(responses);
        let mut connection = connection_running_notebooks_at(endpoint);
        let mut statement = bigframes_statement(&mut connection, Some("4242"), "0");

        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::Unauthorized, "{}", error.message);
        assert!(
            error.message.ends_with(&format!(
                "; the BigQuery Vertex AI notebook execution job {JOB} may still be running"
            )),
            "{}",
            error.message
        );
        assert_eq!(captured.join().unwrap().len(), 2);
    }
}

/// `pooling_timeout` is one deadline over both waits. Here the create spends all of it, so the
/// operation wait expires at once and the job is never polled.
#[test]
fn one_deadline_covers_both_waits_and_expiry_stops_the_job() {
    let (endpoint, captured) =
        capture_requests_gated(vec![(200, lro(false)), (200, json!({}))], || {
            thread::sleep(Duration::from_millis(1_500));
            true
        });
    let mut connection = connection_running_notebooks_at(endpoint);
    let mut statement = bigframes_statement(&mut connection, Some("4242"), "1");

    let error = statement.execute_update().unwrap_err();
    assert_eq!(
        error.status,
        Status::Timeout,
        "an expired deadline is a timeout, not a job failure: {}",
        error.message
    );
    for expected in [
        "adbc.bigquery.dataproc.pooling_timeout",
        JOB,
        "1 s",
        "asked to stop",
    ] {
        assert!(
            error.message.contains(expected),
            "a timeout has to name the job left running and the budget that expired, not just \
             the option: {expected} missing from {}",
            error.message
        );
    }

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 2, "the create, then the cancel");
    assert!(
        requests[1]
            .line
            .starts_with(&format!("POST /v1/{LRO}:cancel ")),
        "an expired wait stops the notebook instead of leaving it running and billing: {}",
        requests[1].line
    );
    assert!(
        requests
            .iter()
            .all(|request| request.path() != format!("/v1/{JOB}")),
        "the job poll never got a budget of its own"
    );
}

/// Once the create operation is `done`, cancelling it cannot stop the job it created, and Vertex AI
/// has no method that cancels a notebook execution job. A deadline that expires while the job
/// runs therefore sends no cancel and says the job may still be running, rather than claiming it
/// was asked to stop.
#[test]
fn a_deadline_that_expires_while_the_job_runs_says_it_may_still_be_running() {
    let (endpoint, captured) = crate::test_support::http::capture_requests_responding(3, {
        let mut served = 0;
        move |_| {
            served += 1;
            Some(match served {
                1 => (200, lro(true)),
                2 => {
                    // Outlast the one-second deadline on the job's first poll.
                    thread::sleep(Duration::from_millis(1_500));
                    (200, job_in_state("JOB_STATE_RUNNING"))
                }
                _ => (200, json!({})),
            })
        }
    });
    let mut connection = connection_running_notebooks_at(endpoint.clone());
    let mut statement = bigframes_statement(&mut connection, Some("4242"), "1");

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::Timeout, "{}", error.message);
    assert_message(&error, &[Key::DataprocPoolingTimeout.as_str(), "1 s"]);
    assert!(
        error.message.ends_with(&format!(
            "; the BigQuery Vertex AI notebook execution job {JOB} may still be running"
        )),
        "{}",
        error.message
    );

    // A cancel would have reached the server before `execute_update` returned, so the next
    // request it sees is this probe exactly when none was sent.
    let mut probe = std::net::TcpStream::connect(endpoint.trim_start_matches("http://")).unwrap();
    std::io::Write::write_all(
        &mut probe,
        b"GET /probe HTTP/1.1\r\nHost: probe\r\nContent-Length: 0\r\n\r\n",
    )
    .unwrap();
    let requests = captured.join().unwrap();
    assert_eq!(requests[1].path(), format!("/v1/{JOB}"));
    assert_eq!(
        requests[2].line, "GET /probe HTTP/1.1",
        "a done operation cannot be cancelled: {}",
        requests[2].line
    );
}

/// The deadline is also checked between the template listing and the create, so a listing that
/// outlives `pooling_timeout` creates no job. Nothing exists yet, so the message must not claim a
/// job was stopped.
#[test]
fn a_deadline_spent_listing_the_runtime_templates_never_creates_the_job() {
    let (endpoint, captured) = capture_requests_gated(
        vec![(
            200,
            json!({"notebookRuntimeTemplates": [{"name": TEMPLATE}]}),
        )],
        || {
            thread::sleep(Duration::from_millis(1_500));
            true
        },
    );
    let mut connection = connection_running_notebooks_at(endpoint);
    let mut statement = bigframes_statement(&mut connection, None, "1");

    let error = statement.execute_update().unwrap_err();
    assert_eq!(
        error.status,
        Status::Timeout,
        "the budget was spent before anything was submitted: {}",
        error.message
    );
    assert_message(&error, &[Key::DataprocPoolingTimeout.as_str(), "1 s"]);
    assert!(
        !error.message.contains("asked to stop"),
        "no job was created, so none was asked to stop: {}",
        error.message
    );

    let requests = captured.join().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "the listing spent the budget; the create must not draw a second one"
    );
}

#[test]
fn a_pooling_timeout_beyond_the_clock_waits_out_both_phases() {
    let (endpoint, captured) = capture_requests(vec![
        (200, lro(true)),
        (200, job_in_state("JOB_STATE_SUCCEEDED")),
    ]);
    let mut connection = connection_running_notebooks_at(endpoint);
    let mut statement = bigframes_statement(&mut connection, Some("4242"), "9223372036854775807");

    assert_eq!(statement.execute_update().unwrap(), None);

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 2, "the create, then one job poll");
    assert_eq!(requests[1].path(), format!("/v1/{JOB}"));
    assert!(
        requests
            .iter()
            .all(|request| !request.line.contains(":cancel")),
        "no deadline expired, so there was nothing to stop"
    );
}

#[test]
fn cancelling_a_notebook_job_stops_the_wait() {
    let request_seen = Arc::new(Barrier::new(2));
    let server_seen = request_seen.clone();
    let (endpoint, captured) = capture_requests_gated(vec![(200, lro(false))], move || {
        server_seen.wait();
        true
    });
    let mut connection = connection_running_notebooks_at(endpoint);
    let mut statement = bigframes_statement(&mut connection, Some("4242"), "3600");
    let handle = statement.get_cancel_handle();
    let canceller = thread::spawn(move || {
        request_seen.wait();
        handle.try_cancel().unwrap();
    });

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::Cancelled, "{}", error.message);
    canceller.join().unwrap();
    assert_eq!(
        captured.join().unwrap().len(),
        1,
        "the poll loop stopped instead of continuing to poll"
    );
}

/// A half-configured job stays active so it names the option it is missing, rather than
/// falling through to the SQL path and complaining about a missing query.
#[test]
fn a_half_configured_notebook_job_names_the_option_it_is_missing() {
    let mut statement = test_statement();
    set_option(&mut statement, Key::NotebookExecuteJobParent, PARENT);
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_message(&error, &["adbc.bigquery.notebook_execute_job.gsc_path"]);

    for (option, value) in [
        (Key::NotebookExecuteJobGscPath, "gs://dbt-python/a/m.py"),
        (Key::NotebookExecuteJobGscBucket, "dbt-python"),
        (Key::NotebookExecuteJobModelFileName, "a/m.py"),
        (Key::NotebookExecuteJobModelName, "m"),
        (Key::NotebookExecuteJobProject, "data-project"),
        (Key::NotebookExecuteJobRegion, "us-central1"),
    ] {
        set_option(&mut statement, option, value.to_owned());
    }
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    for expected in [
        "Vertex AI notebook execution job",
        "adbc.bigquery.create_batch.batch_id",
        "display name",
        "submission_method: bigframes",
        "Colab Enterprise console",
    ] {
        assert!(
            error.message.contains(expected),
            "the key is Dataproc-namespaced, so the message has to say whose option it is and \
             what it is for: {expected} missing from {}",
            error.message
        );
    }
}

#[test]
fn terminal_job_states_are_classified_and_running_ones_keep_polling() {
    for running in [
        "JOB_STATE_UNSPECIFIED",
        "JOB_STATE_QUEUED",
        "JOB_STATE_PENDING",
        "JOB_STATE_RUNNING",
        "JOB_STATE_CANCELLING",
        "JOB_STATE_PAUSED",
        "JOB_STATE_UPDATING",
    ] {
        assert!(
            job_outcome(running, &json!({}), JOB).is_none(),
            "{running} is not terminal, so the poll loop must keep waiting"
        );
    }
    assert!(
        job_outcome("JOB_STATE_SUCCEEDED", &json!({}), JOB)
            .unwrap()
            .is_ok()
    );

    let cancelled = job_outcome("JOB_STATE_CANCELLED", &json!({}), JOB)
        .unwrap()
        .unwrap_err();
    assert_eq!(cancelled.status, Status::Cancelled);
    let expired = job_outcome("JOB_STATE_EXPIRED", &json!({}), JOB)
        .unwrap()
        .unwrap_err();
    assert_eq!(expired.status, Status::Timeout);

    // With no `pooling_timeout`, an unclassifiable state must end the wait or it could poll
    // forever.
    for (unrecognized, detail) in [
        (
            "JOB_STATE_SOMETHING_NEW",
            "the unrecognized state JOB_STATE_SOMETHING_NEW",
        ),
        ("", "reported no jobState"),
    ] {
        let failure = job_outcome(unrecognized, &json!({}), JOB)
            .unwrap()
            .unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_message(&failure, &[JOB, detail]);
        assert!(
            failure.message.contains("JOB_STATE_RUNNING"),
            "the message names the states that would have kept the wait going: {}",
            failure.message
        );
    }
    // A failure the service reported no status for stays honestly unknown.
    let bare = job_outcome("JOB_STATE_FAILED", &json!({"name": JOB}), JOB)
        .unwrap()
        .unwrap_err();
    assert_eq!(bare.status, Status::Unknown);
    assert_message(&bare, &["no failure detail"]);
}

#[test]
fn notebook_identity_can_be_derived_overridden_and_cleared() {
    const EXPLICIT: &str = "notebooks@data-project.iam.gserviceaccount.com";
    for (connect, derived) in [
        (
            connection_running_notebooks_at as fn(String) -> BigQueryConnection,
            None,
        ),
        (
            connection_running_notebooks_as_a_service_account,
            Some(SERVICE_ACCOUNT),
        ),
    ] {
        for (overrides, expected) in [
            (&[][..], derived),
            (&[EXPLICIT][..], Some(EXPLICIT)),
            (&[EXPLICIT, ""][..], derived),
        ] {
            let (endpoint, captured) = capture_requests(vec![
                (200, lro(true)),
                (200, job_in_state("JOB_STATE_SUCCEEDED")),
            ]);
            let mut connection = connect(endpoint);
            let mut statement = bigframes_statement(&mut connection, Some("4242"), "3600");
            for &value in overrides {
                set_option(&mut statement, Key::NotebookExecuteJobServiceAccount, value);
            }
            assert_string_option(
                &statement,
                Key::NotebookExecuteJobServiceAccount,
                overrides.last().copied().filter(|value| !value.is_empty()),
            );

            assert_eq!(statement.execute_update().unwrap(), None);

            let requests = captured.join().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[0].body.get("serviceAccount"),
                expected.map(|value| json!(value)).as_ref(),
                "derived {derived:?}, override sequence {overrides:?}"
            );
            assert!(
                requests[0].body.get("executionUser").is_none(),
                "execution_identity is a oneof: {}",
                requests[0].body
            );
        }
    }
}

/// The notebook `parent`, `project`, and `region` reach the request path, and the region also
/// the host, so a value that would send credentials elsewhere is refused, as for Dataproc.
#[test]
fn resource_options_that_would_redirect_the_request_are_refused() {
    for (option, value) in [
        (Key::NotebookExecuteJobRegion, "us-central1.example.com/v1"),
        (Key::NotebookExecuteJobParent, "projects/p?alt=media"),
        (Key::NotebookExecuteJobProject, "data project"),
    ] {
        // A port nothing listens on: the option must be refused before any request is made.
        let mut connection = connection_running_notebooks_at("http://127.0.0.1:1".to_owned());
        let mut statement = bigframes_statement(&mut connection, Some("4242"), "3600");
        set_option(&mut statement, option, value.to_owned());
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{value}");
        assert_message(&error, &[option.as_str()]);
    }
}

#[test]
fn resource_names_keep_their_segments_and_escape_the_rest() {
    assert_eq!(escape(PARENT).to_string(), PARENT);
    assert_eq!(
        escape("projects/p/locations/l/notebookExecutionJobs/a b").to_string(),
        "projects/p/locations/l/notebookExecutionJobs/a%20b",
        "a segment cannot smuggle a space, a query string or a fragment into the URL"
    );
    assert_eq!(escape("a?b#c").to_string(), "a%3Fb%23c");
}
