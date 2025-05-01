//! This module defines the implementation of the `UserDefinedLogicalNodeCore` trait for the `Sample` logical plan node.
//!
//! The `Sample` node represents a custom logical plan extension for sampling data within a query plan.
//!
use std::{hash::Hash, vec};

use datafusion::{
    error::DataFusionError,
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Sample {
    pub fraction: f32,
    pub seed: Option<i64>,
    pub input: LogicalPlan,
}

impl Hash for Sample {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.seed.hash(state);
        self.input.hash(state);
    }
}

impl Eq for Sample {}

impl Sample {
    pub fn new(fraction: f32, seed: Option<i64>, input: LogicalPlan) -> Self {
        Self {
            fraction,
            seed,
            input,
        }
    }
}

impl UserDefinedLogicalNodeCore for Sample {
    fn name(&self) -> &str {
        "Sample"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Sample: fraction: {}, seed: {:?}",
            self.fraction, self.seed
        ))?;
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            seed: self.seed,
            fraction: self.fraction,
            input: inputs
                .first()
                .ok_or(DataFusionError::Plan("expected single input".to_string()))?
                .clone(),
        })
    }
}
