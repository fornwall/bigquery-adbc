use std::sync::Arc;

use adbc_core::error::Status;
use serde_json::json;

use crate::rest::model::{QueryParameter, QueryParameterType, QueryParameterValue};
use crate::{BigQueryCreateDisposition, BigQueryQueryPriority, BigQueryWriteDisposition};

use super::super::plan::{QueryIdentity, QueryJobCreation, QueryPlan, QueryRoute};
use crate::query::{QualifiedTableName, QueryDefaults, StatementQueryOptions};
use crate::test_support::errors::assert_message_omits;
use crate::test_support::http::job_reference;
use crate::test_support::identifiers::project;

fn plan(
    options: &StatementQueryOptions,
    defaults: QueryDefaults<'_>,
    location: Option<&str>,
    session_id: Option<&str>,
    parameters: Option<(&str, Vec<QueryParameter>)>,
    job_creation: QueryJobCreation,
) -> QueryPlan {
    QueryPlan::new(
        Some(&project("billing-project")),
        defaults,
        location,
        session_id,
        "SELECT @answer AS answer".into(),
        parameters,
        options,
        job_creation,
    )
    .unwrap()
}

/// The plan the option-free cases use: no defaults, location, session or parameters.
fn simple_plan(options: &StatementQueryOptions) -> QueryPlan {
    plan(
        options,
        QueryDefaults::default(),
        None,
        None,
        None,
        QueryJobCreation::Optional,
    )
}

fn parameter(name: Option<&str>, value: &str) -> QueryParameter {
    QueryParameter {
        name: name.unwrap_or_default().to_owned(),
        parameter_type: Some(QueryParameterType {
            r#type: "INT64".to_owned(),
            ..Default::default()
        }),
        parameter_value: Some(QueryParameterValue {
            value: Some(value.to_owned()),
            ..Default::default()
        }),
    }
}

#[test]
fn default_query_is_the_exact_compact_jobs_query_body() {
    let plan = simple_plan(&StatementQueryOptions::default());
    assert_eq!(plan.route(), QueryRoute::Query);
    let request = plan.jobs_query_request().unwrap();
    assert_eq!(request.request_id(), plan.request_id().unwrap());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.encode().unwrap()).unwrap(),
        json!({
            "query": "SELECT @answer AS answer",
            "useLegacySql": false,
            "requestId": request.request_id(),
            "formatOptions": {"useInt64Timestamp": true},
            "jobCreationMode": "JOB_CREATION_OPTIONAL",
            "timeoutMs": 10_000,
            "maxResults": 100_000
        })
    );
}

#[test]
fn encoding_grows_for_escaped_sql_and_large_parameters() {
    let sql = format!(
        "SELECT '{}' AS value, @answer AS answer",
        "\"\\\n\t中".repeat(1024)
    );
    let value = "parameter \"\\\n\t中".repeat(1024);
    let mut plan = plan(
        &StatementQueryOptions::default(),
        QueryDefaults::default(),
        None,
        None,
        Some(("NAMED", vec![parameter(Some("answer"), &value)])),
        QueryJobCreation::Optional,
    );
    plan.sql = Arc::from(sql.as_str());
    let request = plan.jobs_query_request().unwrap();
    let body = request.encode().unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(body.len() > sql.len() + 512);
    assert_eq!(decoded["query"], sql);
    assert_eq!(
        decoded["queryParameters"][0]["parameterValue"]["value"],
        value
    );
    assert_eq!(decoded["requestId"], request.request_id());
}

#[test]
fn jobs_query_preserves_every_shared_option_and_required_job_mode() {
    let billing = project("billing-project");
    let data = project("data-project");
    let labels = Arc::new(
        [
            ("dbt_invocation_id".to_owned(), "abc-123".to_owned()),
            ("team".to_owned(), "analytics".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    let options = StatementQueryOptions {
        maximum_bytes_billed: 123,
        job_timeout_ms: 456,
        labels: Some(labels),
        create_session: true,
        disable_query_cache: true,
        // These explicit semantic defaults must remain absent and must not force jobs.insert.
        create_disposition: BigQueryCreateDisposition::CreateIfNeeded,
        write_disposition: BigQueryWriteDisposition::WriteEmpty,
        priority: BigQueryQueryPriority::Interactive,
        ..StatementQueryOptions::default()
    };
    let plan = QueryPlan::new(
        Some(&billing),
        QueryDefaults {
            catalog: Some(&data),
            db_schema: Some("data_dataset"),
        },
        Some("EU"),
        Some("session-123"),
        "SELECT @answer AS answer".into(),
        Some(("NAMED", vec![parameter(Some("answer"), "42")])),
        &options,
        QueryJobCreation::Required,
    )
    .unwrap();
    assert_eq!(plan.route(), QueryRoute::Query);
    let request = plan.jobs_query_request().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.encode().unwrap()).unwrap(),
        json!({
            "query": "SELECT @answer AS answer",
            "useLegacySql": false,
            "requestId": request.request_id(),
            "formatOptions": {"useInt64Timestamp": true},
            "jobCreationMode": "JOB_CREATION_REQUIRED",
            "timeoutMs": 10_000,
            "maxResults": 100_000,
            "defaultDataset": {
                "projectId": "data-project",
                "datasetId": "data_dataset"
            },
            "location": "EU",
            "connectionProperties": [{"key": "session_id", "value": "session-123"}],
            "parameterMode": "NAMED",
            "queryParameters": [{
                "name": "answer",
                "parameterType": {"type": "INT64"},
                "parameterValue": {"value": "42"}
            }],
            "maximumBytesBilled": "123",
            "jobTimeoutMs": "456",
            "labels": {
                "dbt_invocation_id": "abc-123",
                "team": "analytics"
            },
            "createSession": true,
            "useQueryCache": false
        })
    );
}

#[test]
fn every_job_only_option_selects_jobs_insert_but_required_job_mode_does_not() {
    let destination = QualifiedTableName::parse_option(
        crate::BigQueryStatementOption::DestinationTable.as_str(),
        "destination-project.destination_dataset.destination_table",
        None,
        None,
    )
    .unwrap();
    let forcing_options = [
        StatementQueryOptions {
            dry_run: true,
            ..StatementQueryOptions::default()
        },
        StatementQueryOptions {
            destination_table: Some(destination),
            ..StatementQueryOptions::default()
        },
        StatementQueryOptions {
            create_disposition: BigQueryCreateDisposition::CreateNever,
            ..StatementQueryOptions::default()
        },
        StatementQueryOptions {
            write_disposition: BigQueryWriteDisposition::WriteAppend,
            ..StatementQueryOptions::default()
        },
        StatementQueryOptions {
            priority: BigQueryQueryPriority::Batch,
            ..StatementQueryOptions::default()
        },
    ];
    for options in &forcing_options {
        assert_eq!(
            simple_plan(options).route(),
            QueryRoute::Insert,
            "{options:?}"
        );
    }

    assert_eq!(
        plan(
            &StatementQueryOptions::default(),
            QueryDefaults::default(),
            None,
            None,
            None,
            QueryJobCreation::Required,
        )
        .route(),
        QueryRoute::Query
    );
}

#[test]
fn domain_scoped_destination_projects_reach_the_job_configuration() {
    let options = StatementQueryOptions {
        destination_table: Some(
            QualifiedTableName::parse_option(
                crate::BigQueryStatementOption::DestinationTable.as_str(),
                "analytics.example.com:project.dataset.table$20240101",
                None,
                None,
            )
            .unwrap(),
        ),
        ..StatementQueryOptions::default()
    };
    let job = simple_plan(&options).jobs_insert_body().unwrap();
    assert_eq!(job["jobReference"]["projectId"], "billing-project");
    assert_eq!(
        job["configuration"]["query"]["destinationTable"],
        json!({
            "projectId": "analytics.example.com:project",
            "datasetId": "dataset",
            "tableId": "table$20240101",
        })
    );
}

#[test]
fn jobs_insert_is_the_exact_compact_job_envelope() {
    let options = StatementQueryOptions {
        destination_table: Some(
            QualifiedTableName::parse_option(
                crate::BigQueryStatementOption::DestinationTable.as_str(),
                "destination-project.destination_dataset.destination_table",
                None,
                None,
            )
            .unwrap(),
        ),
        create_disposition: BigQueryCreateDisposition::CreateNever,
        write_disposition: BigQueryWriteDisposition::WriteAppend,
        maximum_bytes_billed: 123,
        job_timeout_ms: 456,
        labels: Some(Arc::new(
            [
                ("dbt_invocation_id".to_owned(), "abc-123".to_owned()),
                ("team".to_owned(), "analytics".to_owned()),
            ]
            .into_iter()
            .collect(),
        )),
        priority: BigQueryQueryPriority::Batch,
        create_session: true,
        disable_query_cache: true,
        dry_run: true,
        ..StatementQueryOptions::default()
    };
    let data = project("data-project");
    let plan = plan(
        &options,
        QueryDefaults {
            catalog: Some(&data),
            db_schema: Some("data_dataset"),
        },
        Some("EU"),
        Some("session-123"),
        Some(("POSITIONAL", vec![parameter(None, "42")])),
        QueryJobCreation::Required,
    );
    assert_eq!(plan.route(), QueryRoute::Insert);
    let body = plan.jobs_insert_body().unwrap();
    let expected = json!({
        "jobReference": job_reference("billing-project", plan.job_id().unwrap()),
        "configuration": {
            "query": {
                "query": "SELECT @answer AS answer",
                "useLegacySql": false,
                "defaultDataset": {
                    "projectId": "data-project",
                    "datasetId": "data_dataset"
                },
                "connectionProperties": [{"key": "session_id", "value": "session-123"}],
                "parameterMode": "POSITIONAL",
                "queryParameters": [{
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": "42"}
                }],
                "maximumBytesBilled": "123",
                "destinationTable": {
                    "projectId": "destination-project",
                    "datasetId": "destination_dataset",
                    "tableId": "destination_table"
                },
                "createDisposition": "CREATE_NEVER",
                "writeDisposition": "WRITE_APPEND",
                "priority": "BATCH",
                "createSession": true,
                "useQueryCache": false
            },
            "dryRun": true,
            "jobTimeoutMs": "456",
            "labels": {
                "dbt_invocation_id": "abc-123",
                "team": "analytics"
            }
        }
    });
    assert_eq!(body, expected);
}

#[test]
fn empty_parameter_sets_keep_the_mode_but_omit_the_empty_array() {
    for (mode, options) in [
        ("NAMED", StatementQueryOptions::default()),
        (
            "POSITIONAL",
            StatementQueryOptions {
                dry_run: true,
                ..StatementQueryOptions::default()
            },
        ),
    ] {
        let plan = plan(
            &options,
            QueryDefaults::default(),
            None,
            None,
            Some((mode, Vec::new())),
            QueryJobCreation::Optional,
        );
        let value = match plan.jobs_query_request() {
            Some(request) => serde_json::to_value(request).unwrap(),
            None => plan.jobs_insert_body().unwrap(),
        };
        let query = if plan.route() == QueryRoute::Query {
            &value
        } else {
            &value["configuration"]["query"]
        };
        assert_eq!(query["parameterMode"], mode);
        assert!(query.get("queryParameters").is_none(), "{query}");
    }
}

#[test]
fn logical_identities_are_stable_across_clones_and_fresh_across_plans() {
    let options = StatementQueryOptions::default();
    let first = simple_plan(&options);
    let retry = first.clone();
    let next = simple_plan(&options);
    assert_eq!(first.identity(), retry.identity());
    assert_ne!(first.identity(), next.identity());
    assert_eq!(first.identity().as_str().len(), 36);
    assert!(first.identity().as_str().starts_with("req_"));
    assert_eq!(
        serde_json::to_value(first.jobs_query_request().unwrap()).unwrap(),
        serde_json::to_value(retry.jobs_query_request().unwrap()).unwrap()
    );

    let insert_options = StatementQueryOptions {
        priority: BigQueryQueryPriority::Batch,
        ..StatementQueryOptions::default()
    };
    let insert = simple_plan(&insert_options);
    let next_insert = simple_plan(&insert_options);
    assert!(matches!(insert.identity(), QueryIdentity::Job(_)));
    assert!(insert.identity().as_str().starts_with("job_"));
    assert_eq!(insert.identity().as_str().len(), 36);
    assert_ne!(insert.identity(), next_insert.identity());
}

#[test]
fn planning_requires_a_billing_project_without_exposing_the_sql() {
    let secret = "SELECT 'sql-secret-sentinel'";
    let error = QueryPlan::new(
        None,
        QueryDefaults::default(),
        None,
        None,
        secret.into(),
        None,
        &StatementQueryOptions::default(),
        QueryJobCreation::Optional,
    )
    .unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_message_omits(&error, &[secret]);
}
