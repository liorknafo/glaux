# glaux — the all-in-one binary

One process, one port (`4566` by default): an embedded
[fakecloud](https://github.com/faiscadev/fakecloud) control plane plus glaux's
**real** Athena (Apache DataFusion) and **real** Firehose delivery.

```
glaux --athena-output-location s3://results/
# → http://0.0.0.0:4566 serves s3, glue, sqs, sns, iam, sts, ssm,
#   secretsmanager, kms, logs (fakecloud) + athena, firehose (glaux)
```

This crate is **AGPL-3.0-only** because it links fakecloud's crates. The
engine crates it embeds — `glaux-athena`, `glaux-firehose`, `glaux-catalog` —
are Apache-2.0 and never depend on fakecloud (enforced by
`ci/check-no-fakecloud-deps.sh`). Need Athena/Firehose against an *external*
S3/Glue (fakecloud over HTTP, MinIO, real AWS)? That is `glaux-server`.

## How it fits together

```
                 ┌──────────────── glaux process ────────────────────────────┐
 AWS SDK / CLI   │  axum ── fakecloud_core::dispatch ── ServiceRegistry       │
 ───────────────▶│          │                             ├─ s3    (fakecloud)│
   :4566         │          │                             ├─ glue  (fakecloud)│
                 │          │                             ├─ sqs … (fakecloud)│
                 │          │                             ├─ athena   (glaux) │
                 │          │                             └─ firehose (glaux) │
                 │                                              │            │
                 │   InProcessStorage / InProcessGlue  ◀─────────┘            │
                 │   (read fakecloud's S3State / GlueAccounts directly)       │
                 └─────────────────────────────────────────────────────────────┘
```

- **Registry replacement.** fakecloud's `ServiceRegistry::register` keys by
  `service_name()`; registering glaux's `athena` and `firehose` services takes
  the slots fakecloud's stubs would occupy, and fakecloud's dispatcher routes
  `X-Amz-Target: AmazonAthena.*` / `Firehose_20150804.*` to them unchanged.
- **Bridging** (`src/bridge.rs`). `AthenaAwsService` / `FirehoseAwsService`
  implement `fakecloud_core::service::AwsService` by calling the engine
  crates' own `http::dispatch` and converting the axum response, so wire
  behavior is identical to `glaux-server`.
- **In-process S3** (`src/storage.rs`). `InProcessStorage` implements
  `glaux_catalog::StorageBackend` (and an `object_store::ObjectStore` for
  DataFusion) by reading fakecloud's `S3State` under its lock: full, ranged,
  and suffix reads (the Parquet footer pattern) never touch a socket. Writes
  and deletes go through fakecloud's `S3Service` handler with a synthesized
  `AwsRequest` — still in-process — so bucket notifications, versioning, and
  ETags behave exactly as for a wire client.
- **In-process Glue** (`src/glue.rs`). `InProcessGlue` implements
  `glaux_catalog::GlueApi` over fakecloud's `GlueAccounts`; partition
  `Expression` filtering reuses fakecloud's evaluator.
- **Live catalog** (`src/engine.rs`). `LiveCatalogEngine` re-snapshots the
  Glue database/table listings into DataFusion before every query, so a
  table created a millisecond ago is queryable.

The integration test `tests/all_in_one_it.rs` records every HTTP request on
the port and asserts that, while an Athena query over Parquet runs, **zero**
S3 or Glue requests occur — the in-process path is measured, not assumed.

## Configuration

Same story as every glaux binary (`glaux_catalog::GlauxConfig`): TOML file
(`--config`), `GLAUX_*` environment, then CLI flags. `glaux --help` lists the
flags. Two differences from `glaux-server`:

- `--addr` / `GLAUX_ADDR` binds the single port (default `0.0.0.0:4566`).
- `s3_endpoint` / `glue_endpoint` (`GLAUX_S3_ENDPOINT`, `GLAUX_GLUE_ENDPOINT`)
  are **rejected at startup**: this binary always serves S3 and Glue itself.
  Accepting and ignoring them would be silently wrong.

Set `--athena-output-location s3://<bucket>/` (and create the bucket) so
queries that omit `ResultConfiguration` have somewhere to write their CSV.

## Embedded fakecloud services

`s3`, `glue`, `sqs`, `sns`, `iam`, `sts`, `ssm`, `secretsmanager`, `kms`,
`logs` — the services the flagship pipeline and typical data-engineering apps
touch, wired as fakecloud's own `main` wires them for this subset (SNS → SQS
fan-out, S3 notifications → SQS/SNS). fakecloud's `athena` and `firehose` are
deliberately **not** linked: glaux's implementations take their names.

**Not embedded in v0.1:** `events` (EventBridge). `fakecloud-eventbridge`
depends on `fakecloud-lambda`, which pulls `zip → xz2 → lzma-sys`
(`links = "lzma"`); DataFusion's compressed-file support pulls
`liblzma-sys` with the same `links` key, and Cargo refuses to link two
crates claiming the same native library. Until either side changes, an
EventBridge-fronted pipeline uses an external fakecloud for the EventBridge
hop, or `glaux-server` pointed at a full fakecloud. Services with container
runtimes (Lambda, ECS, RDS, …) are out of scope for the embedded binary.

## Upgrading fakecloud

Every `fakecloud-*` dependency is pinned **exactly** (`=0.44.10`) in
`Cargo.toml`, and `FAKECLOUD_VERSION` in `src/server.rs` must match. glaux
reads fakecloud's *internal* state types (`S3State`, `S3Object`,
`GlueAccounts`, `Table`, …) and synthesizes `AwsRequest`s, none of which are
covered by semver guarantees, so an upgrade is a reviewed change:

1. Bump every `fakecloud-*` pin in `crates/glaux/Cargo.toml` to the new
   version (all at once — fakecloud publishes its crates in lockstep) and
   `FAKECLOUD_VERSION` in `src/server.rs`. Also bump
   `crates/glaux-catalog/tests/fakecloud_it.rs` (`FAKECLOUD_VERSION`), which
   runs against the released binary.
2. `cargo fetch`. A `links = "lzma"` conflict means a newly linked crate
   pulls `xz2`; drop that service or wait for upstream.
3. `cargo build -p glaux`. Compile errors point at the state types that
   moved: `src/storage.rs` (`S3State::read_body*`, `S3Object`,
   `S3Bucket::objects`, `AwsRequest` fields, `S3Service::new`),
   `src/glue.rs` (`GlueState::dbs_in`, `Table`, `Partition`,
   `StorageDescriptor`, `partition_filter::matches`), `src/server.rs`
   (service constructors and `DeliveryBus` wiring — diff against the new
   fakecloud `main.rs`, fetched with
   `curl -L https://crates.io/api/v1/crates/fakecloud/<ver>/download | tar xz`).
4. Re-verify the two facts glaux relies on and cannot detect at compile
   time: `ServiceRegistry::register` still *replaces* an existing name
   (fakecloud's `registry.rs` has a `register_overwrites_same_name` test),
   and `protocol.rs` still maps `AmazonAthena` → `athena` and `Firehose_*`
   → `firehose`. `tests/all_in_one_it.rs` fails loudly if either changed.
5. `cargo test -p glaux` and `ci/check-no-fakecloud-deps.sh`.
