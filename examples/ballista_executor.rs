use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_extensions::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};
use std::sync::Arc;
///
/// # Custom Ballista Executor
///
/// This example demonstrates how to crate custom ballista executors with support
/// for custom logical and physical codecs.
///
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    //
    //
    //

    let config: ExecutorProcessConfig = ExecutorProcessConfig {
        override_logical_codec: Some(Arc::new(ExtendedBallistaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(ExtendedBallistaPhysicalCodec::default())),

        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
