//! This module defines the `SampleExec` physical execution plan node for DataFusion.
//! `SampleExec` implements random sampling of input record batches, emitting a subset
//! of rows according to a specified sampling fraction and optional random seed.
//! It wraps another execution plan and filters its output batches using a random mask,
//! enabling approximate query processing or data subsampling in distributed query execution.
//!
//! **NOTE: sampling implementation is just for illustrative purposes**

use datafusion::error::DataFusionError;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, stream::RecordBatchReceiverStreamBuilder,
};
use futures_util::stream::StreamExt;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug)]
pub struct SampleExec {
    pub fraction: f32,
    pub seed: Option<i64>,
    pub input: Arc<dyn ExecutionPlan>,
}

impl SampleExec {
    pub fn new(fraction: f32, seed: Option<i64>, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            fraction,
            seed,
            input,
        }
    }
}

impl DisplayAs for SampleExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for SampleExec {
    fn name(&self) -> &str {
        "SampleExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        let input = children
            .get(0)
            .ok_or(DataFusionError::Internal(
                "single input is expected".to_string(),
            ))?
            .clone();

        Ok(Arc::new(Self {
            input,
            fraction: self.fraction,
            seed: self.seed,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let mut stream = self.input.execute(partition, context)?;
        let mut downstream = RecordBatchReceiverStreamBuilder::new(stream.schema(), 4);

        let tx = downstream.tx();

        let fraction = self.fraction;

        // Create a random number generator, optionally seeded
        let mut rng: StdRng = match self.seed {
            Some(seed) => StdRng::seed_from_u64(seed as u64),
            None => StdRng::from_os_rng(),
        };

        downstream.spawn(async move {
            while let Some(batch) = stream.next().await {
                let result = batch.and_then(|b| {
                    // we can probably get better way of creating sample
                    // using rng.fill, it is good enough for this poc
                    let mask: Vec<bool> = (0..b.num_rows())
                        .map(|_| rng.random_range(0.0..=1.0) < fraction)
                        .collect();

                    let boolean_array = datafusion::arrow::array::BooleanArray::from(mask);

                    datafusion::arrow::compute::filter_record_batch(&b, &boolean_array)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
                });

                tx.send(result).await.unwrap();
            }
            Ok(())
        });

        Ok(downstream.build())
    }
}
