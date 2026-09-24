use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use lakehouse::{
    daily_demand_to_dataframe, daily_sales_gold, read_silver_events_from_parquet, write_parquet,
};

fn main() -> Result<()> {
    let root = PathBuf::from(env::var("DATA_LAKE_ROOT").unwrap_or_else(|_| "data-lake".to_owned()));
    let silver_root = root.join("silver").join("inventory_events");
    let gold_path = root
        .join("gold")
        .join("daily_demand")
        .join("part-0000.parquet");

    let files = parquet_files(&silver_root)?;
    let mut events = Vec::new();
    for file in files {
        events.extend(read_silver_events_from_parquet(&file)?);
    }

    let daily = daily_sales_gold(&events);
    write_parquet(daily_demand_to_dataframe(&daily)?, &gold_path)?;

    println!(
        "wrote {} daily demand rows to {}",
        daily.len(),
        gold_path.display()
    );
    Ok(())
}

fn parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    collect_parquet_files(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_parquet_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, files)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            files.push(path);
        }
    }
    Ok(())
}
