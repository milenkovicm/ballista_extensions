//! This module provides a custom query planner for DataFusion that supports user-defined logical nodes and their corresponding physical execution plans.
//!
//! Specifically, it introduces `QueryPlannerWithExtensions`, which wraps the default DataFusion physical planner and injects a custom extension planner (`CustomPlannerExtension`).
//! This extension planner enables the translation of custom logical nodes, such as `Sample`, into their physical execution counterparts (`SampleExec`).
//!
//! The module is designed to be extensible, allowing additional custom logical nodes and their physical implementations to be integrated into the query planning process.
//! It leverages DataFusion's extension points to seamlessly support custom query operations within the DataFusion execution framework.
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::{SessionState, context::QueryPlanner},
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};

use crate::{logical::sample_extension::Sample, physical::sample_exec::SampleExec};

pub struct QueryPlannerWithExtensions {
    inner: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for QueryPlannerWithExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPlannerWithExtensions").finish()
    }
}

impl Default for QueryPlannerWithExtensions {
    fn default() -> Self {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            CustomPlannerExtension::default(),
        )]);

        Self { inner: planner }
    }
}

#[async_trait]
impl QueryPlanner for QueryPlannerWithExtensions {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[derive(Debug, Clone, Default)]
pub struct CustomPlannerExtension {}

#[async_trait]
impl ExtensionPlanner for CustomPlannerExtension {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(Sample { fraction, seed, .. }) = node.as_any().downcast_ref::<Sample>() {
            let input = physical_inputs
                .first()
                .ok_or(DataFusionError::Plan("expected single input".to_string()))?
                .clone();
            let node = SampleExec::new(*fraction, *seed, input);
            let node = Arc::new(node);

            // it may make sense to use CoalesceBatchesExec to buffer batches
            //  for sake of simplicity will omit it

            Ok(Some(node))
        } else {
            Ok(None)
        }
    }
}
