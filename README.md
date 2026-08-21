# glaux 🦉

**The local data plane for AWS emulators — real Athena queries and real Firehose delivery, in Rust.**

Local AWS emulators ([fakecloud](https://github.com/faiscadev/fakecloud), RustStack, …) implement the *control plane* of analytics services but stop at the data plane: their Athena returns fabricated rows without executing SQL, and their Firehose accepts records and drops the bytes. glaux fills exactly that gap:

- **Athena** — `StartQueryExecution` / `GetQueryResults` backed by [Apache DataFusion](https://datafusion.apache.org/), executing real SQL (joins, CTEs, subqueries, window functions, aggregations) over real Parquet / JSON-lines / CSV data in S3, with Glue Data Catalog metadata. Results are also written as CSV + `.metadata` to the `OutputLocation`, as on AWS.
- **Firehose** — delivery streams that actually deliver: `BufferingHints`, S3 prefix expressions (`!{timestamp:...}`, `ErrorOutputPrefix`), GZIP, and JSON→Parquet conversion from the Glue table schema.

So this runs end-to-end on your laptop, with zero AWS bill:

```
EventBridge → SQS → your consumer → Firehose → Parquet in S3 → Athena SQL
```

**Never silently wrong.** Any unsupported SQL construct, unmappable type, or unconvertible record fails with an explicit error naming it (`NOT_SUPPORTED: ... is not supported`). No synthesized results, ever. The [SQL coverage table](docs/sql-coverage.md) is generated from the translator's own registry, so it lists exactly what the engine accepts.

## Contents

- [Quickstart](#quickstart) — [all-in-one](#all-in-one-glaux), [standalone](#standalone-glaux-server), [docker-compose](#docker-compose)
- [The pipeline demo](#the-pipeline-demo)
- [SQL coverage](#sql-coverage)
- [Pairing with fakecloud](#pairing-with-fakecloud)
- [Configuration](#configuration)
- [Fidelity](#fidelity)
- [Licensing](#licensing)
- [Building from source and contributing](#building-from-source)

## Quickstart

Two binaries, one product:

| Binary | License | What it is | Port |
|---|---|---|---|
| `glaux` | AGPL-3.0 | All-in-one: an embedded fakecloud control plane (S3, Glue, SQS, SNS, IAM/STS, SSM, Secrets Manager, KMS, Logs) **plus** glaux's Athena and Firehose, on one port. The engines read fakecloud's S3/Glue state in-process — no loopback hops. | 4566 |
| `glaux-server` | Apache-2.0 | Standalone Athena + Firehose over HTTP; point it at *any* S3/Glue endpoint: fakecloud, MinIO, or real AWS. | 4570 |

Install either from the [GitHub releases](https://github.com/liorknafo/glaux/releases) (tarballs for macOS and Linux, arm64 and amd64, each containing both binaries), from the container images, or with cargo:

```sh
# Release tarball (pick your platform: darwin-arm64, darwin-amd64, linux-arm64, linux-amd64)
curl -sSL https://github.com/liorknafo/glaux/releases/latest/download/glaux-v0.1.0-darwin-arm64.tar.gz | tar xz
./glaux-v0.1.0-darwin-arm64/glaux --version

# Containers
docker run --rm -p 4566:4566 ghcr.io/liorknafo/glaux:0.1.0            # all-in-one (AGPL-3.0)
docker run --rm -p 4570:4570 ghcr.io/liorknafo/glaux-server:0.1.0     # standalone (Apache-2.0)

# cargo (Apache crates are on crates.io; the AGPL all-in-one binary builds from this repo)
cargo install glaux-server
cargo install --git https://github.com/liorknafo/glaux glaux
```

Every example below uses dummy credentials; neither binary checks signatures.

```sh
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
```

### All-in-one: `glaux`

```sh
glaux --athena-output-location s3://athena-results/
# listening on http://0.0.0.0:4566 — s3, glue, sqs, sns, iam, sts, ssm,
# secretsmanager, kms, logs (fakecloud) + athena, firehose (glaux)

aws --endpoint-url http://localhost:4566 s3 mb s3://athena-results
QID=$(aws --endpoint-url http://localhost:4566 athena start-query-execution \
  --query-string "SELECT 40 + 2 AS answer, upper('glaux') AS name" \
  --query QueryExecutionId --output text)
aws --endpoint-url http://localhost:4566 athena get-query-results --query-execution-id $QID
```

One port for everything means the SDK configuration is a single `endpoint_url`. Details of what is embedded and how the in-process S3/Glue path works: [`crates/glaux/README.md`](crates/glaux/README.md).

### Standalone: `glaux-server`

`glaux-server` never assumes where S3 and Glue are. It refuses to start without explicit endpoints (or `--aws` for real AWS), and refuses to start when an endpoint it was given does not answer.

```sh
# against a fakecloud running on 4566
glaux-server --s3-endpoint http://127.0.0.1:4566 --glue-endpoint http://127.0.0.1:4566
aws --endpoint-url http://localhost:4570 athena list-work-groups
aws --endpoint-url http://localhost:4570 firehose list-delivery-streams
curl -s http://localhost:4570/health     # reports which endpoints it is running against

# against real AWS: real queries over your real S3 data, without paying Athena per query
glaux-server --aws --region eu-west-1 --athena-output-location s3://my-scratch-bucket/results/
```

The [`examples/`](examples/README.md) walkthrough creates a Glue table, a Firehose stream converting JSON to Parquet, and queries the result, all with the AWS CLI.

### docker-compose

[`examples/docker-compose.yml`](examples/docker-compose.yml) runs fakecloud (S3, Glue, and 100+ control-plane services) on 4566 and `glaux-server` (Athena, Firehose) on 4570:

```sh
docker compose -f examples/docker-compose.yml up --build -d
aws --endpoint-url http://localhost:4566 s3 mb s3://lake                        # fakecloud
aws --endpoint-url http://localhost:4570 athena list-work-groups                # glaux-server
```

## The pipeline demo

[`examples/flagship-pipeline.sh`](examples/flagship-pipeline.sh) scripts the whole flow with the AWS CLI and `jq`, and checks that what comes out of Athena is exactly what went into EventBridge:

```sh
examples/flagship-pipeline.sh          # builds glaux with cargo if GLAUX_BIN is unset
```

```
==> starting fakecloud (EventBridge + SQS) on http://127.0.0.1:4567
==> starting glaux glaux 0.1.0 (fakecloud 0.44.10) on http://127.0.0.1:4566
==> S3 buckets and the Glue table the Parquet conversion and Athena both use
==> Firehose delivery stream: JSON in, Parquet out (schema from Glue shop.orders)
==> SQS queue and an EventBridge rule routing source=shop.orders to it
==> publishing 6 OrderPlaced events
==> consumer: draining SQS, forwarding event details to Firehose PutRecordBatch
==> waiting for Firehose to deliver Parquet under s3://lake/orders/
    s3://lake/orders/2026/08/21/03/orders-1-2026-08-21-03-32-10-b32c39f9-....parquet
==> Athena: SELECT country, count(*) AS orders, round(sum(amount), 2) AS revenue FROM shop.orders GROUP BY country ORDER BY country
    country  orders  revenue
    DE       2       61.0
    FR       1       40.5
    US       3       111.5
    results CSV: 4 lines at s3://athena-results/ff8432f8-....csv
==> pipeline OK: 6 events, EventBridge -> SQS -> Firehose -> Parquet -> Athena
```

What runs where: `glaux` serves S3, Glue, Firehose, and Athena; a separate fakecloud process serves the EventBridge → SQS hop, because fakecloud's EventBridge crate cannot be linked into the same binary as DataFusion today (both pull a native `lzma` — see [`crates/glaux/README.md`](crates/glaux/README.md#embedded-fakecloud-services)). The consumer is the twenty lines a real service would own: receive, unwrap the EventBridge envelope, `PutRecordBatch`, delete.

The same flow is an integration test — [`crates/glaux/tests/flagship_pipeline_it.rs`](crates/glaux/tests/flagship_pipeline_it.rs) — that CI runs on every pull request, and the release workflow runs the script against the freshly built release binary before anything is published.

## SQL coverage

[`docs/sql-coverage.md`](docs/sql-coverage.md) is generated from the Trino→DataFusion shim registry: every SQL construct, every function (passthrough, rewritten, or implemented as a Rust UDF), and every construct that is **refused by name**, with notes on semantic differences that were matched deliberately (NULL ordering, `_colN` naming, integer division, cast rounding, …). If it is not in the table, the query fails with an error naming the construct.

v0.1 scope is the full analytical `SELECT` surface over Parquet, JSON-lines, and CSV tables, including Hive-style partitions and partition projection. Writes (`CTAS`, `INSERT INTO`, `UNLOAD`) are v0.2; Iceberg is v0.3 — see the [design spec](docs/specs/2026-08-15-glaux-v0.1-design.md).

## Pairing with fakecloud

glaux *consumes* S3 and Glue, it does not implement them. Three ways to pair it with fakecloud:

| Setup | When | How |
|---|---|---|
| **All-in-one `glaux`** | Most local development and CI. One process, one port, in-process S3/Glue reads. | `glaux`. Embeds fakecloud `0.44.10`'s S3, Glue, SQS, SNS, IAM/STS, SSM, Secrets Manager, KMS, Logs. fakecloud's own Athena and Firehose stubs are replaced by glaux's. |
| **`glaux-server` + fakecloud over HTTP** | You need a fakecloud service glaux does not embed (EventBridge, Lambda, DynamoDB, …), or you already run fakecloud. | Run fakecloud on 4566 and `glaux-server --s3-endpoint http://fakecloud:4566 --glue-endpoint http://fakecloud:4566` on 4570; point SDK clients for Athena/Firehose at 4570 and everything else at 4566 ([compose file](examples/docker-compose.yml)). |
| **Both** | Full pipelines such as the demo above: EventBridge on fakecloud, data plane on `glaux`. | Two processes, two endpoints — [`examples/flagship-pipeline.sh`](examples/flagship-pipeline.sh) is the template. |

`glaux-server` also works against MinIO (S3) plus any Glue-compatible catalog, and against real AWS (`--aws`). Ranged S3 reads are required (Parquet footers); fakecloud, MinIO, and S3 all support them.

## Configuration

One configuration story for both binaries, in order of precedence: defaults → TOML file (`--config`) → `GLAUX_*` environment → flags. `glaux --help` / `glaux-server --help` list the flags.

| Setting | Env | Notes |
|---|---|---|
| Listen address | `GLAUX_ADDR` (glaux) / `GLAUX_LISTEN` (glaux-server) | defaults `0.0.0.0:4566` / `0.0.0.0:4570` |
| S3 / Glue endpoints | `GLAUX_S3_ENDPOINT`, `GLAUX_GLUE_ENDPOINT` | required by `glaux-server` (or `--aws`); **rejected** by `glaux`, which always serves them itself |
| Region / account | `GLAUX_REGION`, `GLAUX_ACCOUNT_ID` | `us-east-1` / `123456789012` |
| Credentials for the endpoints | `GLAUX_ACCESS_KEY_ID`, `GLAUX_SECRET_ACCESS_KEY`, `GLAUX_SESSION_TOKEN` | only `glaux-server`; `--aws` uses the standard AWS credential chain |
| Athena defaults | `GLAUX_ATHENA_OUTPUT_LOCATION`, `GLAUX_ATHENA_WORKGROUP` | queries without a `ResultConfiguration` need an output location |
| Firehose limits | `GLAUX_FIREHOSE_MAX_{RECORD_KIB,BATCH_RECORDS,BATCH_MIB}` | AWS defaults: 1024 KiB per record, 500 records and 4 MiB per batch |

## Fidelity

Results are compared with real Athena, not with what seemed reasonable. [`crates/glaux-fidelity`](crates/glaux-fidelity) is the differential suite: the SQL corpus under [`crates/glaux-athena/tests/corpus`](crates/glaux-athena/tests/corpus) runs against glaux over Parquet/NDJSON/CSV fixtures and is diffed against recorded snapshots on every CI run (`cargo run -p glaux-fidelity -- replay`). `cargo run -p glaux-fidelity -- record --profile <aws-profile>` re-records the snapshots against real AWS Athena in a scratch bucket and database that are torn down afterwards; snapshots not yet recorded against AWS are marked `UNVERIFIED` in their header and in the coverage table. Firehose artifacts (object keys, Parquet contents) are snapshotted the same way.

## Licensing

| Crate | License | On crates.io |
|---|---|---|
| `glaux-athena` — Athena API + DataFusion execution | Apache-2.0 | yes |
| `glaux-firehose` — Firehose API + delivery engine | Apache-2.0 | yes |
| `glaux-catalog` — Glue client, DataFusion catalog provider, S3 backends | Apache-2.0 | yes |
| `glaux-server` — standalone binary | Apache-2.0 | yes |
| `glaux` — all-in-one binary | AGPL-3.0-only | no (release tarballs and `ghcr.io/liorknafo/glaux`) |

Why two licenses: fakecloud is AGPL-3.0, and the all-in-one binary links its crates, so it is AGPL too — which for a local development tool you run on your own machine or CI changes nothing for you. Everything that is glaux's own work — the engines, the catalog layer, and the standalone server — is Apache-2.0 and has **zero** fakecloud dependencies, transitively; [`ci/check-no-fakecloud-deps.sh`](ci/check-no-fakecloud-deps.sh) enforces that boundary on every build. Embed the Apache crates in your own tools freely; use `glaux-server` if AGPL is a problem in your environment.

## Building from source

```sh
git clone https://github.com/liorknafo/glaux && cd glaux
cargo build --release                       # target/release/{glaux,glaux-server}
cargo test --workspace                      # unit + integration tests (fakecloud-backed tests download a release binary)
cargo run -p glaux-fidelity -- replay       # differential suite, offline
examples/flagship-pipeline.sh               # the pipeline, end to end
```

Rust 1.90+. The first build pulls DataFusion and takes a few minutes. Regenerate the coverage table with `GLAUX_REGEN_DOCS=1 cargo test -p glaux-athena coverage_doc`; CI fails if it is stale.

Releases are cut by pushing a `v*` tag: [`release.yml`](.github/workflows/release.yml) builds the four platform tarballs, runs the pipeline demo against the Linux binary, publishes the two container images, and creates the GitHub release; [`publish.yml`](.github/workflows/publish.yml) publishes the Apache crates to crates.io on manual dispatch.

## Status

v0.1: Athena `SELECT` surface, Firehose direct-PUT delivery with Parquet conversion, both binaries, the differential suite, and the flagship pipeline test. Roadmap in the [design spec](docs/specs/2026-08-15-glaux-v0.1-design.md#roadmap).
