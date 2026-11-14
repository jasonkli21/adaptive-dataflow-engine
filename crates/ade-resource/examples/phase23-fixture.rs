//! Materialize a literal Parquet fixture or decode native IPC for the campaign.
#[path = "../tests/support/fixture.rs"]
mod fixture;

use arrow::ipc::reader::StreamReader;
use serde_json::{Value, json};
use std::{fs::File, path::Path};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let root = Path::new(&args[2]);
    let result = match args[1].as_str() {
        "aggregate" => fixture::spec(root, false, &fixture::rows("left"), &[]),
        "join" => fixture::spec(root, true, &fixture::rows("left"), &fixture::rows("right")),
        "empty" => fixture::spec(root, false, &[], &[]),
        "boundary" => fixture::spec(root, true, &vec![(5, 13); 1025], &[(5, -7)]),
        "read" => {
            let reader = StreamReader::try_new(File::open(root).unwrap(), None).unwrap();
            let schema = reader.schema();
            let fields: Vec<_> = schema.fields.iter().map(|field| json!({
                "name":field.name(), "type":format!("{:?}",field.data_type()), "nullable":field.is_nullable(),
            })).collect();
            let mut rows = Vec::new();
            for batch in reader {
                let batch = batch.unwrap();
                for row in 0..batch.num_rows() {
                    rows.push(
                        (0..batch.num_columns())
                            .map(|col| {
                                arrow::util::display::array_value_to_string(batch.column(col), row)
                                    .unwrap()
                                    .parse::<i64>()
                                    .unwrap()
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            }
            rows.sort_unstable();
            json!({"schema":fields,"rows":rows,"blake3":ade_physical::fingerprint(&std::fs::read(root).unwrap())})
        }
        _ => Value::Null,
    };
    println!("{result}");
}
