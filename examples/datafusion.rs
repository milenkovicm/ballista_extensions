use std::sync::Arc;

use ballista_extensions::{
    dataframe::sample::DataFrameExt, planner::extension_planner::QueryPlannerWithExtensions,
};
use datafusion::{common::Result, execution::SessionStateBuilder, prelude::SessionContext};

// this example is with datafusion integration
// to test it works without running it on cluster

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let query_planner = Arc::new(QueryPlannerWithExtensions::default());

    let state = SessionStateBuilder::new()
        .with_query_planner(query_planner)
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);
    let df = ctx.read_parquet("data/", Default::default()).await?;

    let df = df.sample(0.40, Some(42))?;

    df.show().await?;

    Ok(())
}
