# CDC Snapshot Table Name Parsing Bug Repro Bundle

This bundle reproduces the historical CDC snapshot table-source parsing bug in a real Databricks deployment.

## What this tests

The dataflow uses historical snapshot mode with a 2-part table name in `cdcSnapshotSettings.source.table`:

- `bug_repro.customer_historical_snapshot_source`

In a Databricks environment with a default catalog, this should resolve as `<default_catalog>.bug_repro.customer_historical_snapshot_source`.

Current framework behavior may incorrectly split and rebuild this as:

- `bug_repro.customer_historical_snapshot_source.customer_historical_snapshot_source`

which causes a runtime table resolution error.

## Pre-deploy setup in Databricks SQL

Run in your target catalog before pipeline execution:

```sql
CREATE SCHEMA IF NOT EXISTS bug_repro;

CREATE OR REPLACE TABLE bug_repro.customer_historical_snapshot_source (
	CUSTOMER_ID STRING,
	FIRST_NAME STRING,
	LAST_NAME STRING,
	EMAIL STRING,
	LOAD_VERSION INT
);

INSERT INTO bug_repro.customer_historical_snapshot_source VALUES
	('1', 'Ada', 'Lovelace', 'ada@example.com', 1),
	('2', 'Alan', 'Turing', 'alan@example.com', 1);
```

## Deploy and run

1. Update [databricks.yml](databricks.yml) variables:
- `workspace_host`
- `framework_source_path`
- optionally `catalog` and `schema`

2. Deploy:

```bash
databricks bundle deploy --target dev
```

3. Run pipeline:

```bash
databricks bundle run cdc_snapshot_table_name_bug_pipeline --target dev
```

## Expected outcome (bug reproduced)

The run should fail during historical table snapshot read with a not-found or invalid identifier error referencing a malformed table path with the table segment duplicated.
