//! The corpus fixture tables, shared by the engine-level corpus run and
//! the service-level results-fidelity run.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, ListBuilder, RecordBatch,
    StringArray, StringBuilder, TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::NaiveDate;

pub fn days(y: i32, m: u32, d: u32) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (NaiveDate::from_ymd_opt(y, m, d).unwrap() - epoch).num_days() as i32
}

pub fn millis(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, mi, s)
        .unwrap()
        .and_utc()
        .timestamp_millis()
}

pub fn string_list(values: Vec<Option<Vec<Option<&str>>>>) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in values {
        match row {
            Some(items) => {
                for item in items {
                    builder.values().append_option(item);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

/// `customers`: id, name, country, signup_date, tags (array<varchar>),
/// profile (JSON text).
pub fn customers() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("signup_date", DataType::Date32, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("profile", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Carol"),
                Some("Dan"),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("US"),
                Some("DE"),
                Some("US"),
                Some("FR"),
                Some("DE"),
            ])),
            Arc::new(Date32Array::from(vec![
                Some(days(2023, 1, 15)),
                Some(days(2023, 6, 30)),
                Some(days(2024, 1, 31)),
                Some(days(2024, 2, 29)),
                None,
            ])),
            string_list(vec![
                Some(vec![Some("vip"), Some("early")]),
                Some(vec![Some("trial")]),
                Some(vec![]),
                Some(vec![Some("vip"), None, Some("beta")]),
                None,
            ]),
            Arc::new(StringArray::from(vec![
                Some(r#"{"age": 34, "plan": "pro", "address": {"city": "NYC", "zip": "10001"}, "scores": [10, 20, 30]}"#),
                Some(r#"{"age": 28, "plan": "free", "address": {"city": "Berlin"}, "scores": []}"#),
                Some(r#"{"age": null, "plan": "pro", "flags": {"beta": true}}"#),
                Some(r#"{"age": 45, "plan": "enterprise", "scores": [1]}"#),
                None,
            ])),
        ],
    )
    .unwrap();
    (schema, batch)
}

/// `orders`: id, customer_id, amount, status, created_at (timestamp ms),
/// note.
pub fn orders() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("status", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("note", DataType::Utf8, true),
        Field::new("rush", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![
                101, 102, 103, 104, 105, 106, 107, 108,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(2),
                Some(3),
                Some(3),
                Some(3),
                Some(9),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(120.5),
                Some(35.0),
                Some(80.25),
                Some(15.0),
                Some(240.0),
                None,
                Some(60.0),
                Some(10.0),
            ])),
            Arc::new(StringArray::from(vec![
                Some("shipped"),
                Some("shipped"),
                Some("pending"),
                Some("cancelled"),
                Some("shipped"),
                Some("pending"),
                Some("shipped"),
                None,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(millis(2024, 1, 5, 10, 30, 0)),
                Some(millis(2024, 1, 31, 23, 59, 59)),
                Some(millis(2024, 2, 14, 8, 0, 0)),
                Some(millis(2024, 3, 1, 0, 0, 0)),
                Some(millis(2024, 3, 15, 12, 0, 0)),
                Some(millis(2024, 12, 31, 18, 45, 10)),
                Some(millis(2025, 1, 1, 0, 0, 0)),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("Order #101: gift wrap, ref ABC-123"),
                Some("  padded  "),
                Some("Überweisung"),
                Some("a,b,,c"),
                Some("2024-03-15 12:00:00"),
                Some("no-digits"),
                Some("x=1;y=22;z=333"),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(false),
                Some(false),
                None,
            ])),
        ],
    )
    .unwrap();
    (schema, batch)
}

/// `events`: id, at (timestamp with nanosecond precision, as a Glue
/// `timestamp` column read from Parquet micros/nanos arrives).
pub fn events() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("at", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]));
    let base = millis(2024, 1, 5, 10, 0, 0) * 1_000_000;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(base + 999_600_000),
                Some(base + 123_456_789),
                Some(base + 500_000),
                None,
            ])),
        ],
    )
    .unwrap();
    (schema, batch)
}
