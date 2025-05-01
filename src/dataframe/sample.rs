use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    logical_expr::{Extension, LogicalPlan},
    prelude::DataFrame,
};

use crate::logical::sample_extension::Sample;

pub trait DataFrameExt {
    fn sample(self, fraction: f32, seed: Option<i64>) -> datafusion::error::Result<DataFrame>;
}

/// Returns a new `DataFrame` containing a random sample of rows from the original `DataFrame`.
///
/// # Arguments
///
/// * `fraction` - The fraction of rows to sample, must be in the range (0.0, 1.0].
/// * `seed` - An optional seed for the random number generator to ensure reproducibility.
///
/// # Errors
///
/// Returns a `DataFusionError::Configuration` if `fraction` is not within the valid range.
///
impl DataFrameExt for DataFrame {
    fn sample(self, fraction: f32, seed: Option<i64>) -> datafusion::error::Result<DataFrame> {
        if !(fraction > 0.0 && fraction <= 1.0) {
            Err(DataFusionError::Configuration(
                "fraction should be in 0 ..= 1 range".to_string(),
            ))?
        }

        if seed.unwrap_or(0) < 0 {
            Err(DataFusionError::Configuration(
                "seed should be positive number".to_string(),
            ))?
        }

        let (state, input) = self.into_parts();

        let node = Arc::new(Sample {
            fraction,
            seed,
            input,
        });
        let extension = Extension { node };
        let plan = LogicalPlan::Extension(extension);

        Ok(DataFrame::new(state, plan))
    }
}
