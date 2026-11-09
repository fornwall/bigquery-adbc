"""Python model covering the driver's dbt 2.0 python-model path.

dbt 2.0 runs two fresh statements on one connection, each with the SQL `SELECT 1` that
the driver's options must override:

1. `adbc.bigquery.write_gcs.{bucket,object_name,content}` uploads the compiled model (this
   source plus dbt's `py_script_postfix`) to `gs://{gcs_bucket}/{schema}/{alias}.py`.
2. `submission_method: cluster` submits a PySpark job to `dataproc_cluster_name` through
   `adbc.bigquery.dataproc.*` and waits for it.

The profile names a cluster that does not exist, so the upload is real and the submission
fails at Dataproc. The runner checks the uploaded object and that the node failed at the job
submission, not on an unsupported option.
"""

# The runner requires this string in the uploaded object; nothing else in the project produces it.
EXOTIC_PYTHON_MODEL_MARKER = "uploaded through adbc.bigquery.write_gcs"


def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="cluster",
        tags=["python_model"],
    )
    # The job never runs, but dbt 2.0 resolves the ref while compiling, so the uploaded
    # object carries the fully qualified relation.
    orders = dbt.ref("fct_orders")
    return orders.groupBy("customer_id").sum("order_total")
