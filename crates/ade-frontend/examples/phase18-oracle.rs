//! Independent `DataFusion` reference executable for acceptance, never a runtime fallback.
use datafusion::prelude::{ParquetReadOptions, SessionContext};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("DATASET SQL".into());
    }
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
        std::path::Path::new(&args[0]).join("manifest.json"),
    )?)?;
    let files = manifest["files"]
        .as_array()
        .ok_or("manifest files")?
        .iter()
        .map(|f| {
            std::path::Path::new(&args[0])
                .join(f["relative_path"].as_str().unwrap_or_default())
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    let ctx = SessionContext::new();
    let view = ctx
        .read_parquet(files, ParquetReadOptions::default())
        .await?
        .into_view();
    ctx.register_table("input", view)?;
    let batches = ctx.sql(&args[1]).await?.collect().await?;
    let mut rows = vec![];
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|col| arrow::util::display::array_value_to_string(col, row))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    rows.sort();
    println!("{}", serde_json::to_string(&rows)?);
    Ok(())
}
