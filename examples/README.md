# glaux-server + fakecloud demo

`docker-compose.yml` runs fakecloud (S3, Glue, …) on port 4566 and
glaux-server (Athena, Firehose) on port 4570. The flow below is the
pipeline's tail end — JSON records in through Firehose, Parquet out into S3,
SQL over it through Athena — with nothing but the AWS CLI.

```sh
docker compose -f examples/docker-compose.yml up --build -d

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
FC=http://localhost:4566   # fakecloud: S3 + Glue
GX=http://localhost:4570   # glaux-server: Athena + Firehose

# 1. Storage and catalog (fakecloud).
aws --endpoint-url $FC s3 mb s3://lake
aws --endpoint-url $FC glue create-database --database-input Name=analytics
aws --endpoint-url $FC glue create-table --database-name analytics --table-input '{
  "Name": "events", "TableType": "EXTERNAL_TABLE",
  "StorageDescriptor": {
    "Columns": [{"Name":"id","Type":"bigint"},{"Name":"name","Type":"string"},{"Name":"amount","Type":"double"}],
    "Location": "s3://lake/events/",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
  }}'

# 2. A Firehose stream converting JSON -> Parquet with the Glue schema (glaux-server).
aws --endpoint-url $GX firehose create-delivery-stream --delivery-stream-name events \
  --extended-s3-destination-configuration '{
    "RoleARN": "arn:aws:iam::000000000000:role/firehose",
    "BucketARN": "arn:aws:s3:::lake", "Prefix": "events/", "ErrorOutputPrefix": "errors/",
    "BufferingHints": {"SizeInMBs": 64, "IntervalInSeconds": 60},
    "DataFormatConversionConfiguration": {
      "Enabled": true,
      "SchemaConfiguration": {"DatabaseName": "analytics", "TableName": "events"},
      "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
      "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {}}}
    }}'

# 3. Records in.
aws --endpoint-url $GX firehose put-record-batch --delivery-stream-name events --records \
  "Data=$(echo -n '{"id":1,"name":"signup","amount":0}' | base64)" \
  "Data=$(echo -n '{"id":2,"name":"purchase","amount":19.99}' | base64)"

# 4. Wait for the 60s buffering interval (or delete the stream to flush now),
#    then the Parquet object is in S3:
aws --endpoint-url $FC s3 ls --recursive s3://lake/events/

# 5. Real SQL over it (glaux-server).
QID=$(aws --endpoint-url $GX athena start-query-execution \
  --query-string "SELECT name, sum(amount) AS total FROM analytics.events GROUP BY name ORDER BY total DESC" \
  --result-configuration OutputLocation=s3://lake/results/ --query 'QueryExecutionId' --output text)
aws --endpoint-url $GX athena get-query-execution --query-execution-id $QID --query 'QueryExecution.Status.State'
aws --endpoint-url $GX athena get-query-results --query-execution-id $QID
```

An unsupported construct fails the query with an explicit error naming it
(`NOT_SUPPORTED: ... is not supported`) — never a fabricated row.

## Configuration

Every setting can come from a TOML file (`--config`), `GLAUX_*` environment
variables, or flags, in that order of precedence. `glaux-server --help` lists
the flags; the variables are `GLAUX_LISTEN`, `GLAUX_S3_ENDPOINT`,
`GLAUX_GLUE_ENDPOINT`, `GLAUX_REGION`, `GLAUX_ACCOUNT_ID`,
`GLAUX_ACCESS_KEY_ID` / `GLAUX_SECRET_ACCESS_KEY` / `GLAUX_SESSION_TOKEN`,
`GLAUX_ATHENA_OUTPUT_LOCATION`, `GLAUX_ATHENA_WORKGROUP`, and
`GLAUX_FIREHOSE_MAX_{RECORD_KIB,BATCH_RECORDS,BATCH_MIB}`.

Endpoints are never assumed: the server refuses to start without S3 and
Glue endpoints (or an explicit `--aws`), and refuses to start when an
endpoint it was given does not answer.
