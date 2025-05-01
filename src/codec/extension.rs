//! This module provides custom extension codecs for Ballista and DataFusion logical and physical plans.
//!
//! # Overview
//!
//! The main purpose of this module is to enable serialization and deserialization of custom logical and physical plan nodes
//! (such as [Sample] and [SampleExec]) for distributed query execution using Ballista. It achieves this by implementing
//! the [LogicalExtensionCodec] and [PhysicalExtensionCodec] traits, allowing custom nodes to be encoded and decoded
//! to and from protocol buffer messages.
//!
//! # Main Components
//!
//! - [ExtendedBallistaLogicalCodec]: Implements [LogicalExtensionCodec] to handle encoding and decoding of custom logical
//!   plan nodes, specifically the `Sample` node. It delegates to the inner Ballista codec for other node types.
//!
//! - [ExtendedBallistaPhysicalCodec]: Implements [PhysicalExtensionCodec] to handle encoding and decoding of custom physical
//!   plan nodes, specifically the `SampleExec` node. It also supports opaque forwarding for unknown to it node types.
//!   Opaque node types should be handled by built in [BallistaPhysicalExtensionCodec]
//!
//! - **Protobuf Messages**: Uses generated protobuf messages ([LMessage], [LSample], [PMessage], [PSample]) to represent
//!   custom logical and physical nodes during serialization.
//!
//! # Usage
//!
//! These codecs are intended to be registered with Ballista and DataFusion to support distributed execution of queries
//! containing custom extension nodes.
use std::sync::Arc;

use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::{common::plan_err, error::DataFusionError};
use datafusion_proto::{
    logical_plan::LogicalExtensionCodec, physical_plan::PhysicalExtensionCodec,
};
use prost::Message;

use crate::{logical::sample_extension::Sample, physical::sample_exec::SampleExec};

use super::messages::{LMessage, LSample, PMessage, PSample, l_message::Extension};

#[derive(Debug, Default)]
pub struct ExtendedBallistaLogicalCodec {
    inner: BallistaLogicalExtensionCodec,
}

//
// Logical Codec Extension
//
impl LogicalExtensionCodec for ExtendedBallistaLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        let message =
            LMessage::decode(buf).map_err(|e| DataFusionError::Internal(e.to_string()))?;

        match message.extension {
            Some(Extension::Sample(sample)) => {
                let node = Arc::new(Sample {
                    input: inputs
                        .first()
                        .ok_or(DataFusionError::Plan("expected input".to_string()))?
                        .clone(),
                    seed: sample.seed,
                    fraction: sample.fraction,
                });

                Ok(datafusion::logical_expr::Extension { node })
            }
            None => plan_err!("Can't cast logical extension "),
        }
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(Sample { seed, fraction, .. }) = node.node.as_any().downcast_ref::<Sample>() {
            let sample = LSample {
                seed: *seed,
                fraction: *fraction,
            };
            let message = LMessage {
                extension: Some(super::messages::l_message::Extension::Sample(sample)),
            };

            message
                .encode(buf)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;

            Ok(())
        } else {
            self.inner.try_encode(node, buf)
        }
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::sql::TableReference,
        schema: datafusion::arrow::datatypes::SchemaRef,
        ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: std::sync::Arc<dyn datafusion::catalog::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }
}

//
//
// Physical Codec Extension
//
//

#[derive(Debug, Default)]
pub struct ExtendedBallistaPhysicalCodec {
    inner: BallistaPhysicalExtensionCodec,
}

impl PhysicalExtensionCodec for ExtendedBallistaPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        let message =
            PMessage::decode(buf).map_err(|e| DataFusionError::Internal(e.to_string()))?;

        match message.extension {
            Some(super::messages::p_message::Extension::Sample(PSample {
                fraction, seed, ..
            })) => {
                let input = inputs
                    .first()
                    .ok_or(DataFusionError::Plan("expected input".to_string()))?
                    .clone();

                let node = Arc::new(SampleExec::new(fraction, seed, input));

                Ok(node)
            }

            Some(super::messages::p_message::Extension::Opaque(opaque)) => {
                self.inner.try_decode(&opaque, inputs, registry)
            }
            None => plan_err!("Can't cast physical extension "),
        }
    }

    fn try_encode(
        &self,
        node: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(SampleExec { fraction, seed, .. }) = node.as_any().downcast_ref::<SampleExec>()
        {
            let message = PMessage {
                extension: Some(super::messages::p_message::Extension::Sample(PSample {
                    fraction: *fraction,
                    seed: *seed,
                })),
            };

            message
                .encode(buf)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;

            Ok(())
        } else {
            let mut opaque = vec![];
            self.inner
                .try_encode(node, &mut opaque)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;

            let message = PMessage {
                extension: Some(super::messages::p_message::Extension::Opaque(opaque)),
            };

            message
                .encode(buf)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;

            Ok(())
        }
    }
}
