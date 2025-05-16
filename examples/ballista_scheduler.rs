use ballista_core::error::BallistaError;
use ballista_extensions::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};
use ballista_extensions::planner::extension_planner::QueryPlannerWithExtensions;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionConfig;
use std::net::AddrParseError;
use std::sync::Arc;

///
/// # Custom Ballista Scheduler
///
/// This example demonstrates how to crate custom ballista schedulers with support
/// for custom logical and physical codecs.
///
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let config: SchedulerConfig = SchedulerConfig {
        override_logical_codec: Some(Arc::new(ExtendedBallistaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(ExtendedBallistaPhysicalCodec::default())),
        override_session_builder: Some(Arc::new(extended_state_producer)),
        ..Default::default()
    };

    let address = format!("{}:{}", config.bind_host, config.bind_port);
    let address = address
        .parse()
        .map_err(|e: AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, address, Arc::new(config)).await?;

    Ok(())
}

pub fn extended_state_producer(config: SessionConfig) -> datafusion::error::Result<SessionState> {
    // we need custom query planner to convert logical to physical operator
    let query_planner = Arc::new(QueryPlannerWithExtensions::default());

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_query_planner(query_planner)
        .with_default_features()
        .build();

    Ok(state)
}
