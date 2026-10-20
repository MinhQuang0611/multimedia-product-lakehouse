# Airflow orchestration layout

The Tiki crawler implementation lives in `scripts/crawl/tiki.py`. There is no
Airflow DAG or scheduler configuration yet.

| Path | Purpose |
|---|---|
| `dags/` | Future Airflow DAG definitions and schedules. |
| `scripts/crawl/` | Tiki crawl implementation and future crawl entrypoints. |
| `scripts/ingest/` | Future ingestion entrypoints, from crawl snapshots into the lake. |
| `scripts/transform/` | Future lake transformations. |
| `scripts/watermark/` | Shared watermark read/claim/commit helpers. |
| `state/` | Local development state only; ignored except for its placeholder. |

## Watermark contract

Watermarks must be durable and shared by Airflow workers. The implementation will store them in MinIO, not in an Airflow task's local filesystem, under:

`s3://lake/_control/watermarks/<pipeline>/<source>/<dataset>.json`

Each future crawl, ingest, or transform must use the following lifecycle:

1. Read the last committed watermark.
2. Derive a deterministic input identity (for example source snapshot date plus content hash).
3. Claim that identity before processing; a concurrent run that sees an active claim must skip it.
4. Write output atomically.
5. Commit the watermark only after output is complete; on failure, leave the old committed watermark unchanged.

The watermark record will include `pipeline`, `source`, `dataset`, `input_id`, `status`, `claimed_at`, `completed_at`, `run_id`, and output checksum/count. This makes every stage idempotent and prevents duplicate crawl, ingestion, and transformation work.
