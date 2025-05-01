#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::{
        assert_batches_eq, execution::SessionStateBuilder, physical_plan::displayable,
        prelude::SessionContext,
    };
    use datafusion_proto::bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
        physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    };

    use ballista_extensions::{
        codec::extension::{ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec},
        dataframe::sample::DataFrameExt,
        planner::extension_planner::QueryPlannerWithExtensions,
    };

    #[tokio::test]
    async fn should_execute_sample() -> datafusion::error::Result<()> {
        let ctx = context();
        let df = ctx
            .sql("select unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]) as a")
            .await?;
        let result = df.sample(0.5, Some(33))?.collect().await?;

        let expected = vec![
            "+---+", "| a |", "+---+", "| 2 |", "| 4 |", "| 9 |", "| 0 |", "+---+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_be_in_range() -> datafusion::error::Result<()> {
        let ctx = context();
        let df = ctx
            .sql("select unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]) as a")
            .await?;

        assert!(df.clone().sample(0.0, Some(33)).is_err());
        assert!(df.clone().sample(-0.01, Some(33)).is_err());
        assert!(df.clone().sample(1.01, Some(33)).is_err());
        assert!(df.clone().sample(1.0, Some(33)).is_ok());
        assert!(df.sample(1.0, Some(-1)).is_err());

        Ok(())
    }

    #[tokio::test]
    async fn should_round_trip_logical_plan() -> datafusion::error::Result<()> {
        let ctx = context();
        let codec = ExtendedBallistaLogicalCodec::default();
        let df = ctx
            .sql("select unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]) as a")
            .await?;
        let result = df.sample(0.5, Some(33))?;

        let plan = result.logical_plan();
        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec)?;
        let new_plan = logical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &codec)?;

        assert_eq!(plan, &new_plan);

        Ok(())
    }

    #[tokio::test]
    async fn should_round_trip_physical_plan() -> datafusion::error::Result<()> {
        let ctx = context();
        let codec = ExtendedBallistaPhysicalCodec::default();
        let df = ctx
            .sql("select unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]) as a")
            .await?;
        let result = df.sample(0.5, Some(33))?;

        let plan = result.create_physical_plan().await?;
        let bytes = physical_plan_to_bytes_with_extension_codec(plan.clone(), &codec)?;
        let new_plan = physical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &codec)?;

        let plan_formatted = format!("{}", displayable(plan.as_ref()).indent(false));
        let new_plan_formatted = format!("{}", displayable(new_plan.as_ref()).indent(false));

        assert_eq!(plan_formatted, new_plan_formatted);

        Ok(())
    }

    fn context() -> SessionContext {
        let query_planner = Arc::new(QueryPlannerWithExtensions::default());

        let state = SessionStateBuilder::new()
            .with_query_planner(query_planner)
            .with_default_features()
            .build();

        SessionContext::new_with_state(state)
    }
}
