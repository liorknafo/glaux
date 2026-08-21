# glaux 🦉

**The local data plane for AWS emulators — real Athena queries and real Firehose delivery, in Rust.**

Local AWS emulators ([fakecloud](https://github.com/faiscadev/fakecloud), RustStack, …) implement the *control plane* of analytics services but stop at the data plane: their Athena returns fake rows without executing SQL, and their Firehose accepts records and drops the bytes. glaux fills exactly that gap:

- **Athena** — `StartQueryExecution` / `GetQueryResults` backed by [Apache DataFusion](https://datafusion.apache.org/), executing real SQL (joins, CTEs, window functions, aggregations) over real Parquet/JSON/CSV data in S3, with Glue Data Catalog metadata.
- **Firehose** — delivery streams that actually deliver: buffering, S3 prefix rules, and JSON→Parquet conversion via Glue schemas.

So this runs end-to-end on your laptop, with zero AWS bill:

```
EventBridge → SQS → your service → Firehose → Parquet in S3 → Athena SQL
```

**Never silently wrong:** any unsupported SQL construct fails loudly with an explicit error. No synthesized results, ever.

## Two ways to run

| Binary | License | What it is |
|---|---|---|
| `glaux` | AGPL-3.0 | All-in-one: embeds fakecloud (105 AWS services) + glaux's Athena & Firehose, one port |
| `glaux-server` | Apache-2.0 | Standalone Athena + Firehose; point it at any S3/Glue endpoint (fakecloud, MinIO, real AWS) |

The engine crates (`glaux-athena`, `glaux-firehose`, `glaux-catalog`) are Apache-2.0 and have no fakecloud dependency.

## Quick start (standalone)

```sh
cargo run -p glaux-server -- --s3-endpoint http://127.0.0.1:4566 --glue-endpoint http://127.0.0.1:4566
aws --endpoint-url http://127.0.0.1:4570 athena list-work-groups
aws --endpoint-url http://127.0.0.1:4570 firehose list-delivery-streams
```

`glaux-server` refuses to start without explicit S3 and Glue endpoints (or `--aws`), and refuses to start when an endpoint it was given does not answer. Configuration comes from a TOML file (`--config`), `GLAUX_*` environment variables, and flags, in that order; `GET /health` reports what it is running against. A docker-compose pairing with fakecloud and a full CLI walkthrough live in [`examples/`](examples/README.md).

## Fidelity

`crates/glaux-fidelity` is the differential suite: the SQL corpus (`crates/glaux-athena/tests/corpus`) runs against glaux over Parquet/NDJSON/CSV fixtures and is diffed against recorded snapshots on every CI run (`cargo run -p glaux-fidelity -- replay`). `cargo run -p glaux-fidelity -- record --profile <aws-profile>` re-records the snapshots against real AWS Athena in a scratch bucket/database that is torn down afterwards; snapshots that have not been recorded against AWS yet are marked `UNVERIFIED` in their header and in [`docs/sql-coverage.md`](docs/sql-coverage.md).

## Status

v0.1 in progress. The all-in-one `glaux` binary runs: an embedded fakecloud control plane (S3, Glue, SQS, SNS, IAM/STS, SSM, Secrets Manager, KMS, Logs) plus glaux's Athena and Firehose on one port, with the engines reading fakecloud's S3/Glue state in-process — see [`crates/glaux/README.md`](crates/glaux/README.md). The v0.1 design spec lives in [`docs/specs/2026-08-15-glaux-v0.1-design.md`](docs/specs/2026-08-15-glaux-v0.1-design.md). Feasibility spikes (ranged S3 reads against fakecloud, embedding custom services on fakecloud's dispatcher) passed on 2026-08-15.
