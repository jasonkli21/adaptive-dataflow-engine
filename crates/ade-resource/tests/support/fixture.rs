//! Independent literal rows converted to native v1 test inputs.
#![allow(
    dead_code,
    reason = "Shared with the standalone fixture and live test."
)]
use ade_physical::{Field, Operator, PlanV1, Source, SourceFile, Target, Type, fingerprint};
use arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
    UInt64Array,
};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};

pub fn literals() -> Value {
    serde_json::from_str(include_str!(
        "../../../../docs/evidence/phase23/fixture.json"
    ))
    .unwrap()
}

pub fn rows(name: &str) -> Vec<(u64, i64)> {
    serde_json::from_value(literals()[name].clone()).unwrap()
}

fn write(path: &Path, fields: &[Field], rows: &[(u64, i64)], join: bool) {
    let keys: ArrayRef = Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0)));
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1)));
    let arrays: Vec<ArrayRef> = if join {
        vec![keys, values]
    } else {
        let n = rows.len();
        vec![
            Arc::new(UInt64Array::from_iter_values(0..n as u64)),
            keys.clone(),
            keys,
            values,
            Arc::new(Float64Array::from(vec![1.0; n])),
            Arc::new(UInt32Array::from(vec![1; n])),
            Arc::new(StringArray::from(vec!["phase23"; n])),
            Arc::new(TimestampMillisecondArray::from(vec![0; n])),
        ]
    };
    let schema = ade_exec::physical::arrow_schema(&fields.to_vec());
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn source_file(
    root: &Path,
    name: &str,
    rows: &[(u64, i64)],
    fields: &[Field],
    join: bool,
) -> SourceFile {
    let path = root.join(name);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    write(&path, fields, rows, join);
    let bytes = fs::read(path).unwrap();
    SourceFile {
        name: name.into(),
        bytes: bytes.len() as u64,
        rows: rows.len() as u64,
        fingerprint: fingerprint(&bytes),
    }
}

pub fn spec(root: &Path, join: bool, left: &[(u64, i64)], right: &[(u64, i64)]) -> Value {
    fs::create_dir_all(root).unwrap();
    let fields = if join {
        vec![
            Field::new("key", Type::U64, false),
            Field::new("value_i64", Type::I64, false),
        ]
    } else {
        vec![
            Field::new("row_id", Type::U64, false),
            Field::new("key", Type::U64, false),
            Field::new("secondary", Type::U64, false),
            Field::new("value_i64", Type::I64, false),
            Field::new("value_f64", Type::F64, false),
            Field::new("category", Type::U32, false),
            Field::new("payload", Type::Utf8, false),
            Field::new("event_time", Type::TimestampMs, false),
        ]
    };
    let (target, tables, schema, schema_id, files) = if join {
        (
            Target::DistributedJoin,
            vec!["ade_left", "ade_right"],
            "synthetic-join-v1",
            fingerprint(
                b"left-v1:key=u64,value_i64=i64;right-v1:key=u64,value_i64=i64;all_nonnull",
            ),
            vec![
                source_file(root, "left/left.parquet", left, &fields, true),
                source_file(root, "right/right.parquet", right, &fields, true),
            ],
        )
    } else {
        (Target::DistributedAggregate, vec!["input"], "synthetic-events-v1",
         fingerprint(b"synthetic-events-v1:row_id=u64,key=u64,secondary=u64,value_i64=i64,value_f64=f64,category=u32,payload=utf8,event_time=timestamp_ms;all_nonnull"),
         vec![source_file(root, "input.parquet", left, &fields, false)])
    };
    let file_entries: Vec<_> = files
        .iter()
        .map(|f| {
            json!({
                "relative_path":f.name,"bytes":f.bytes,"rows":f.rows,"fingerprint":f.fingerprint
            })
        })
        .collect();
    let row_count = files.iter().map(|f| f.rows).sum::<u64>();
    let logical = fingerprint(&serde_json::to_vec(&file_entries).unwrap());
    let manifest = serde_json::to_vec(&json!({
        "schema_version":1,"dataset_version":1,"required_schema":schema,
        "schema_fingerprint":schema_id,"logical_fingerprint":logical,
        "format":"parquet","row_count":row_count,"files":file_entries,
    }))
    .unwrap();
    let path = root.join("manifest.json");
    fs::write(&path, &manifest).unwrap();
    let sources = tables
        .iter()
        .enumerate()
        .map(|(i, &table)| Source {
            id: u32::try_from(i).unwrap(),
            table: table.into(),
            manifest: fingerprint(&manifest),
            logical: logical.clone(),
            schema_identity: schema_id.clone(),
            schema: fields.clone(),
            files: vec![files[i].clone()],
            replayable: !join,
        })
        .collect();
    let mut plan = PlanV1::new(target, sources);
    plan.push(Operator::Scan { source: 0 }).unwrap();
    if join {
        plan.push(Operator::Scan { source: 1 }).unwrap();
        plan.push(Operator::InnerJoin {
            left: 0,
            right: 1,
            left_key: 0,
            right_key: 0,
            left_value: 1,
            right_value: 1,
        })
        .unwrap();
    } else {
        plan.push(Operator::GroupSumCount {
            input: 0,
            key: 1,
            value: 3,
        })
        .unwrap();
    }
    let inputs: Vec<_> = tables
        .iter()
        .map(|table| json!({"table":table, "root":root,"manifest":path}))
        .collect();
    json!({"version":1, "plan":plan, "inputs":inputs, "priority":0, "work":1,
        "deadline_ms":20_000,"runtime_bytes":1_048_576,"staging_bytes":65_536,"preemptible":true})
}
