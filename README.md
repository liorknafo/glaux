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

## Status

Pre-implementation. The v0.1 design spec lives in [`docs/specs/2026-08-15-glaux-v0.1-design.md`](docs/specs/2026-08-15-glaux-v0.1-design.md). Feasibility spikes (ranged S3 reads against fakecloud, embedding custom services on fakecloud's dispatcher) passed on 2026-08-15.
