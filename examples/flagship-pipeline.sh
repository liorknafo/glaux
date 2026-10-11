#!/usr/bin/env bash
# The flagship pipeline, end to end, with nothing but the AWS CLI and jq:
#
#   EventBridge PutEvents -> rule -> SQS -> consumer -> Firehose PutRecordBatch
#     -> Parquet in S3 -> Athena SELECT returns the events
#
# Two processes serve it:
#
#   fakecloud  (EventBridge + SQS)                 http://127.0.0.1:$FAKECLOUD_PORT
#   glaux      (S3, Glue, Firehose, Athena, ...)   http://127.0.0.1:$GLAUX_PORT
#
# The all-in-one glaux binary embeds fakecloud's S3/Glue/SQS/SNS/... but not
# EventBridge (see crates/glaux/README.md, "Embedded fakecloud services"), so
# the EventBridge hop runs on a separate fakecloud process. Everything after
# the consumer — Firehose buffering, JSON->Parquet conversion with the Glue
# schema, the S3 write, real SQL — is glaux.
#
# The script is also the release gate: CI runs it against the freshly built
# release binary (.github/workflows/release.yml) and it exits non-zero unless
# Athena returns exactly the events that went in.
#
# Usage:
#   examples/flagship-pipeline.sh            # builds glaux with cargo if GLAUX_BIN is unset
#   GLAUX_BIN=/path/to/glaux examples/flagship-pipeline.sh
#
# Environment:
#   GLAUX_BIN        glaux binary (default: `glaux` on PATH, else cargo build --release -p glaux)
#   FAKECLOUD_BIN    fakecloud binary (default: `fakecloud` on PATH, else downloaded from GitHub releases)
#   FAKECLOUD_VERSION  release to download when FAKECLOUD_BIN is unset (default v0.44.10)
#   GLAUX_PORT / FAKECLOUD_PORT  ports to bind (defaults 4566 / 4567)
#   EVENT_COUNT      number of OrderPlaced events to publish (default 6)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GLAUX_PORT="${GLAUX_PORT:-4566}"
FAKECLOUD_PORT="${FAKECLOUD_PORT:-4567}"
FAKECLOUD_VERSION="${FAKECLOUD_VERSION:-v0.44.10}"
EVENT_COUNT="${EVENT_COUNT:-6}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_PAGER=""
export AWS_EC2_METADATA_DISABLED=true

FC="http://127.0.0.1:${FAKECLOUD_PORT}"
GX="http://127.0.0.1:${GLAUX_PORT}"

log() { printf '\033[1;34m==> %s\033[0m\n' "$*" >&2; }
die() { printf '\033[1;31merror: %s\033[0m\n' "$*" >&2; exit 1; }

for tool in aws jq curl; do
    command -v "$tool" >/dev/null || die "$tool is required"
done

# ---------------------------------------------------------------- binaries --
if [[ -z "${GLAUX_BIN:-}" ]]; then
    if command -v glaux >/dev/null; then
        GLAUX_BIN="$(command -v glaux)"
    else
        log "GLAUX_BIN unset and no glaux on PATH: building with cargo (release)"
        (cd "$REPO_ROOT" && cargo build --release -p glaux)
        GLAUX_BIN="$REPO_ROOT/target/release/glaux"
    fi
fi
[[ -x "$GLAUX_BIN" ]] || die "glaux binary not executable: $GLAUX_BIN"

if [[ -z "${FAKECLOUD_BIN:-}" ]]; then
    if command -v fakecloud >/dev/null; then
        FAKECLOUD_BIN="$(command -v fakecloud)"
    else
        case "$(uname -s)-$(uname -m)" in
            Darwin-arm64) platform=darwin-arm64 ;;
            Darwin-x86_64) platform=darwin-amd64 ;;
            Linux-aarch64) platform=linux-arm64 ;;
            Linux-x86_64) platform=linux-amd64 ;;
            *) die "no fakecloud release for $(uname -s)-$(uname -m); set FAKECLOUD_BIN" ;;
        esac
        cache="${TMPDIR:-/tmp}/glaux-fakecloud-${FAKECLOUD_VERSION}"
        FAKECLOUD_BIN="$cache/fakecloud-${FAKECLOUD_VERSION}-${platform}/fakecloud"
        if [[ ! -x "$FAKECLOUD_BIN" ]]; then
            log "downloading fakecloud ${FAKECLOUD_VERSION} (${platform}) for the EventBridge hop"
            mkdir -p "$cache"
            curl -sSL --fail --max-time 300 \
                "https://github.com/faiscadev/fakecloud/releases/download/${FAKECLOUD_VERSION}/fakecloud-${FAKECLOUD_VERSION}-${platform}.tar.gz" \
                | tar -xzf - -C "$cache"
        fi
    fi
fi
[[ -x "$FAKECLOUD_BIN" ]] || die "fakecloud binary not executable: $FAKECLOUD_BIN"

# ---------------------------------------------------------------- processes --
WORK="$(mktemp -d "${TMPDIR:-/tmp}/glaux-pipeline.XXXXXX")"
pids=()
cleanup() {
    for pid in "${pids[@]:-}"; do
        [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

wait_http() { # url, label
    for _ in $(seq 1 150); do
        if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
        sleep 0.2
    done
    die "$2 did not become healthy at $1 (log: $WORK)"
}

log "starting fakecloud (EventBridge + SQS) on $FC"
"$FAKECLOUD_BIN" --addr "127.0.0.1:${FAKECLOUD_PORT}" >"$WORK/fakecloud.log" 2>&1 &
pids+=("$!")
wait_http "$FC/_fakecloud/health" fakecloud

log "starting glaux $("$GLAUX_BIN" --version | head -1) on $GX"
RUST_LOG="${RUST_LOG:-info}" "$GLAUX_BIN" --addr "127.0.0.1:${GLAUX_PORT}" \
    --athena-output-location s3://athena-results/ >"$WORK/glaux.log" 2>&1 &
pids+=("$!")
wait_http "$GX/_glaux/health" glaux

# ---------------------------------------------- 1. storage + catalog (glaux) --
log "S3 buckets and the Glue table the Parquet conversion and Athena both use"
aws --endpoint-url "$GX" s3 mb s3://lake >/dev/null
aws --endpoint-url "$GX" s3 mb s3://athena-results >/dev/null
aws --endpoint-url "$GX" glue create-database --database-input Name=shop
aws --endpoint-url "$GX" glue create-table --database-name shop --table-input '{
  "Name": "orders", "TableType": "EXTERNAL_TABLE",
  "StorageDescriptor": {
    "Columns": [
      {"Name": "order_id", "Type": "bigint"},
      {"Name": "country",  "Type": "string"},
      {"Name": "amount",   "Type": "double"}
    ],
    "Location": "s3://lake/orders/",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
  }}'

# ------------------------------------------- 2. Firehose stream (glaux) --
log "Firehose delivery stream: JSON in, Parquet out (schema from Glue shop.orders)"
aws --endpoint-url "$GX" firehose create-delivery-stream --delivery-stream-name orders \
  --delivery-stream-type DirectPut \
  --extended-s3-destination-configuration '{
    "RoleARN": "arn:aws:iam::123456789012:role/firehose",
    "BucketARN": "arn:aws:s3:::lake",
    "Prefix": "orders/",
    "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
    "BufferingHints": {"SizeInMBs": 64, "IntervalInSeconds": 0},
    "DataFormatConversionConfiguration": {
      "Enabled": true,
      "SchemaConfiguration": {"DatabaseName": "shop", "TableName": "orders"},
      "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
      "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {}}}
    }}' >/dev/null

# ------------------------------------ 3. EventBridge -> SQS (fakecloud) --
log "SQS queue and an EventBridge rule routing source=shop.orders to it"
QUEUE_URL="$(aws --endpoint-url "$FC" sqs create-queue --queue-name orders --query QueueUrl --output text)"
QUEUE_ARN="$(aws --endpoint-url "$FC" sqs get-queue-attributes --queue-url "$QUEUE_URL" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)"
aws --endpoint-url "$FC" events put-rule --name orders-to-sqs \
    --event-pattern '{"source":["shop.orders"],"detail-type":["OrderPlaced"]}' >/dev/null
aws --endpoint-url "$FC" events put-targets --rule orders-to-sqs \
    --targets "Id=orders-queue,Arn=$QUEUE_ARN" >/dev/null

log "publishing $EVENT_COUNT OrderPlaced events"
countries=(DE US US FR DE US)
entries="$(for i in $(seq 1 "$EVENT_COUNT"); do
    country="${countries[$(( (i - 1) % ${#countries[@]} ))]}"
    jq -cn --arg c "$country" --argjson id "$i" \
        '{Source:"shop.orders",DetailType:"OrderPlaced",Detail:({order_id:$id,country:$c,amount:($id*10+0.5)}|tojson)}'
done | jq -cs .)"
failed="$(aws --endpoint-url "$FC" events put-events --entries "$entries" --query FailedEntryCount --output text)"
[[ "$failed" == "0" ]] || die "PutEvents reported $failed failed entries"

# -------------------------------------------- 4. consumer: SQS -> Firehose --
log "consumer: draining SQS, forwarding event details to Firehose PutRecordBatch"
consumed=0
for _ in $(seq 1 100); do
    messages="$(aws --endpoint-url "$FC" sqs receive-message --queue-url "$QUEUE_URL" \
        --max-number-of-messages 10 --wait-time-seconds 1 --query 'Messages' --output json)"
    count="$(jq 'length' <<<"${messages:-[]}")"
    if [[ "$count" == "0" || "$count" == "null" ]]; then
        (( consumed >= EVENT_COUNT )) && break
        continue
    fi
    # Each SQS body is the EventBridge envelope; the record is its `detail`.
    records="$(jq -c '[.[] | (.Body | fromjson | .detail | tojson + "\n" | @base64) | {Data: .}]' <<<"$messages")"
    put="$(aws --endpoint-url "$GX" firehose put-record-batch --delivery-stream-name orders --records "$records")"
    [[ "$(jq .FailedPutCount <<<"$put")" == "0" ]] || die "PutRecordBatch failures: $put"
    jq -r '.[] | .ReceiptHandle' <<<"$messages" | while read -r handle; do
        aws --endpoint-url "$FC" sqs delete-message --queue-url "$QUEUE_URL" --receipt-handle "$handle"
    done
    consumed=$((consumed + count))
    (( consumed >= EVENT_COUNT )) && break
done
(( consumed == EVENT_COUNT )) || die "consumer forwarded $consumed of $EVENT_COUNT events"

# ------------------------------------------------- 5. Parquet in S3 (glaux) --
log "waiting for Firehose to deliver Parquet under s3://lake/orders/"
keys=""
for _ in $(seq 1 100); do
    keys="$(aws --endpoint-url "$GX" s3api list-objects-v2 --bucket lake --prefix orders/ \
        --query 'Contents[].Key' --output text 2>/dev/null || true)"
    [[ -n "$keys" && "$keys" != "None" ]] && break
    sleep 0.2
done
[[ -n "$keys" && "$keys" != "None" ]] || die "no objects delivered under s3://lake/orders/ (glaux log: $WORK/glaux.log)"
for key in $keys; do
    [[ "$key" == *.parquet ]] || die "delivered object is not Parquet: $key"
    echo "    s3://lake/$key"
done
errors="$(aws --endpoint-url "$GX" s3api list-objects-v2 --bucket lake --prefix errors/ \
    --query 'Contents[].Key' --output text 2>/dev/null || true)"
[[ -z "$errors" || "$errors" == "None" ]] || die "records landed under the error prefix: $errors"

# ------------------------------------------------------ 6. Athena (glaux) --
SQL="SELECT country, count(*) AS orders, round(sum(amount), 2) AS revenue FROM shop.orders GROUP BY country ORDER BY country"
log "Athena: $SQL"
QID="$(aws --endpoint-url "$GX" athena start-query-execution --query-string "$SQL" \
    --query-execution-context Database=shop --query QueryExecutionId --output text)"
for _ in $(seq 1 150); do
    state="$(aws --endpoint-url "$GX" athena get-query-execution --query-execution-id "$QID" \
        --query 'QueryExecution.Status.State' --output text)"
    [[ "$state" == "QUEUED" || "$state" == "RUNNING" ]] || break
    sleep 0.2
done
if [[ "$state" != "SUCCEEDED" ]]; then
    aws --endpoint-url "$GX" athena get-query-execution --query-execution-id "$QID" >&2
    die "query $QID finished $state"
fi
# Rows as [country, orders, revenue] with numbers parsed, so "31.0" and 31 agree.
actual="$(aws --endpoint-url "$GX" athena get-query-results --query-execution-id "$QID" \
    --output json \
    | jq -c '.ResultSet.Rows[1:] | map(.Data | map(.VarCharValue)) | map([.[0], (.[1] | tonumber), ((.[2] | tonumber) * 100 | round)])')"

# Expected rows computed from the same events the script published.
expected="$(for i in $(seq 1 "$EVENT_COUNT"); do
    printf '%s %s\n' "${countries[$(( (i - 1) % ${#countries[@]} ))]}" "$i"
done | jq -Rsc '
    split("\n") | map(select(length > 0) | split(" ") | {country: .[0], amount: ((.[1] | tonumber) * 10 + 0.5)})
    | group_by(.country)
    | map([.[0].country, length, ((map(.amount) | add) * 100 | round)])
    | sort_by(.[0])')"

aws --endpoint-url "$GX" athena get-query-results --query-execution-id "$QID" --output json \
    | jq -r '.ResultSet.Rows[] | .Data | map(.VarCharValue) | "    " + join("\t")'
if [[ "$actual" != "$expected" ]]; then
    echo "actual:   $actual" >&2
    echo "expected: $expected" >&2
    die "Athena result does not match the published events"
fi

# The results CSV also landed in the OutputLocation, as on real Athena.
csv="$(aws --endpoint-url "$GX" s3 cp "s3://athena-results/${QID}.csv" - 2>/dev/null)" \
    || die "results CSV missing at s3://athena-results/${QID}.csv"
echo "    results CSV: $(wc -l <<<"$csv" | tr -d ' ') lines at s3://athena-results/${QID}.csv"

log "pipeline OK: $EVENT_COUNT events, EventBridge -> SQS -> Firehose -> Parquet -> Athena"
