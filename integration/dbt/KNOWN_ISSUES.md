# Current dbt BigQuery integration issues

Issues the integration suite pins, with the dbt version named in [README.md](README.md).

## Nested `persist_docs` never reaches BigQuery

`models/known_issues/schema.yml` documents six nested paths on
`known_issues_contract_nested_types` under `persist_docs={'relation': true, 'columns': true}`.
After `dbt build`, BigQuery holds the three top-level descriptions and none of the nested ones,
because dbt 2.0 re-nests dotted paths by their first segment before calling the driver (UP-77
in [UPSTREAM.md](../../docs/UPSTREAM.md)). `assert_nested_persist_docs` in `run.py` requires the
nested descriptions to stay missing, so it fails once the upstream fix lands.

## Bundled-driver RANGE limitation

The canonical bundled driver returns a native RANGE value through `dbt show --inline`, but building
a model with a RANGE column fails because it reports the GoogleSQL type `RANGE` as unsupported. The
shared [RANGE fixture](models/known_issues/known_issues_current_range.sql) therefore casts its
result to STRING so the build passes under both drivers.

## The canonical driver exits on forced REST results

With `use_storage_api_disabled_client`, the canonical driver's REST conversion has no case for
strings, and it reports the failure with `log.Fatalf`, which ends the dbt process. A non-null
`STRING` value is enough. `--upstream-adbc` therefore passes `canonical_driver` to
`assert_adapter_surface`, which skips its REST pagination statement for that driver.
