use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_extensions::{
    codec::extension::{ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec},
    dataframe::sample::DataFrameExt,
};
use datafusion::{
    common::Result,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        .is_test(true)
        .try_init();

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(ExtendedBallistaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(ExtendedBallistaPhysicalCodec::default()));

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;
    let df = ctx.read_parquet("data/", Default::default()).await?;

    // `sample`` is new operator defined in this project
    // it should sample 30% of data and show it
    let df = df.sample(0.30, None)?;
    df.show().await?;

    Ok(())
}
