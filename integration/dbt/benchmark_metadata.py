"""Compare BASIC/FULL tables.get latency with the driver's configuration projection.

Uses one authenticated HTTP session to isolate the metadata view from transport
differences. Creates scratch resources and removes them when the run finishes.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery

from config import require_project

RESULTS_DIR = Path(__file__).resolve().parent / "logs" / "performance"
FIELDS = (
    "clustering,description,encryptionConfiguration,expirationTime,"
    "externalDataConfiguration,labels,materializedView,maxStaleness,"
    "rangePartitioning,requirePartitionFilter,resourceTags,schema,timePartitioning,type,view"
)


def create_relations(client: bigquery.Client, dataset: str, materialized: bool) -> list[str]:
    base = f"{dataset}.partitioned"
    table = bigquery.Table(
        base,
        schema=[
            bigquery.SchemaField("event_date", "DATE", description="Event partition day"),
            bigquery.SchemaField("group_id", "INTEGER", description="Clustering key"),
            bigquery.SchemaField("value", "INTEGER", description="Aggregated value"),
        ],
    )
    table.time_partitioning = bigquery.TimePartitioning(type_="DAY", field="event_date", expiration_ms=604_800_000)
    table.clustering_fields = ["group_id"]
    table.description = "Compare BASIC and FULL metadata with an identical field projection"
    table.labels = {"purpose": "adbc-metadata-benchmark", "environment": "scratch"}
    client.create_table(table)
    view = bigquery.Table(f"{dataset}.logical_view")
    view.view_query = f"SELECT event_date, group_id, value FROM `{base}`"
    view.view_use_legacy_sql = False
    view.description = "Metadata projection logical view"
    view.labels = {"purpose": "adbc-metadata-benchmark"}
    client.create_table(view)
    relations = ["partitioned", "logical_view"]
    if materialized:
        view = bigquery.Table(f"{dataset}.materialized_view")
        view.mview_query = (
            f"SELECT event_date, group_id, SUM(value) AS total_value FROM `{base}` GROUP BY event_date, group_id"
        )
        view.mview_enable_refresh = False
        view.description = "Metadata projection materialized view"
        client.create_table(view)
        relations.append("materialized_view")
    return relations


def fetch(session: AuthorizedSession, prefix: str, table: str, view: str) -> tuple[Any, float, int]:
    start = time.perf_counter()
    response = session.get(
        f"{prefix}/tables/{quote(table, safe='')}",
        params={"view": view, "fields": FIELDS, "prettyPrint": "false"},
        headers={"Accept-Encoding": "identity"},
        timeout=60,
    )
    elapsed = time.perf_counter() - start
    response.raise_for_status()
    return response.json(), elapsed, len(response.content)


def summarize(samples: list[dict[str, Any]]) -> dict[str, Any]:
    basic = [sample["seconds"] for sample in samples if sample["view"] == "BASIC"]
    full = [sample["seconds"] for sample in samples if sample["view"] == "FULL"]
    return {
        "pairs": len(basic),
        "basic_median_seconds": statistics.median(basic),
        "full_median_seconds": statistics.median(full),
        "median_paired_basic_over_full": statistics.median(b / f for b, f in zip(basic, full, strict=True)),
        "median_paired_saved_seconds": statistics.median(f - b for b, f in zip(basic, full, strict=True)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=int, default=24)
    parser.add_argument("--include-materialized-view", action="store_true")
    args = parser.parse_args()
    if args.pairs < 20:
        parser.error("--pairs must be at least 20")
    project = require_project()
    location = os.environ.get("DBT_BIGQUERY_LOCATION", "US")
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:10]}"
    dataset_id = f"bigquery_adbc_metadata_perf_{run_id}"
    dataset = bigquery.Dataset(f"{project}.{dataset_id}")
    dataset.location = location
    dataset.default_table_expiration_ms = 3_600_000
    dataset.labels = {"purpose": "adbc-metadata-benchmark"}
    prefix = (
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{quote(project, safe='')}"
        f"/datasets/{quote(dataset_id, safe='')}"
    )
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
    session = AuthorizedSession(credentials)
    client = bigquery.Client(project=project, credentials=credentials)
    samples: list[dict[str, Any]] = []
    result: dict[str, Any] = {
        "run_id": run_id,
        "project": project,
        "location": location,
        "dataset": dataset_id,
        "fields": FIELDS,
        "samples": samples,
        "projected_responses_equal": False,
        "cleanup_complete": False,
    }
    created = False
    try:
        client.create_dataset(dataset)
        created = True
        relations = create_relations(client, f"{project}.{dataset_id}", args.include_materialized_view)
        # Warm credentials and the shared connection outside the recorded samples.
        for relation in relations:
            for _ in range(3):
                for view in ("BASIC", "FULL"):
                    fetch(session, prefix, relation, view)
        for pair in range(args.pairs):
            order = ("BASIC", "FULL") if pair % 2 == 0 else ("FULL", "BASIC")
            offset = pair % len(relations)
            for relation in relations[offset:] + relations[:offset]:
                responses = {}
                for position, view in enumerate(order):
                    response, elapsed, size = fetch(session, prefix, relation, view)
                    responses[view] = response
                    samples.append(
                        {
                            "relation": relation,
                            "pair": pair,
                            "position": position,
                            "view": view,
                            "seconds": elapsed,
                            "bytes": size,
                        }
                    )
                if responses["BASIC"] != responses["FULL"]:
                    raise AssertionError(f"Projected responses differ for {relation}, pair {pair}")
            print(f"Completed {pair + 1}/{args.pairs} pairs per relation", flush=True)
        result["projected_responses_equal"] = True
        result["summary"] = {
            relation: summarize([sample for sample in samples if sample["relation"] == relation])
            for relation in relations
        }
        print(json.dumps(result["summary"], indent=2))
    finally:
        try:
            if created:
                client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)
                result["cleanup_complete"] = True
        finally:
            RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            output = RESULTS_DIR / f"metadata-{run_id}.json"
            output.write_text(json.dumps(result, indent=2) + "\n")
            session.close()
            client.close()
            print(f"Saved {output}; cleanup={result['cleanup_complete']}", flush=True)


if __name__ == "__main__":
    main()
