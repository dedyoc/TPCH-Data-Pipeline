import pandas as pd
from dagster import (
    multi_asset,
    AssetIn,
    AssetOut,
    Output,
    asset,
)


def extract_dataset_tpch(table_name, partition):
    @asset(
        name="bronze_" + table_name,
        io_manager_key="minio_io_manager",
        required_resource_keys={"mysql_io_manager"},
        key_prefix=["bronze", "tpch"],
        compute_kind="MySQL",
        group_name="bronze_layer",
        partitions_def=partition,
    )
    def bronze_dataset(context) -> Output[pd.DataFrame]:
        sql_stm = f"SELECT * FROM {table_name} "
        try:
            partition_date_str = context.asset_partition_key_for_output()
            context.log.info(partition_date_str)
            # TODO: your code here, sql_stm query by partition?
            sql_stm += f"""WHERE DATE(O_ORDERDATE) = '{partition_date_str}'"""
        except Exception:
            context.log.info(f"{table_name} has no partition key! Yaya")
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log.info(pd_data.columns)

        return Output(pd_data, metadata={"table": table_name})

    return bronze_dataset


@asset(
    name="bronze_dummy",
    io_manager_key="minduck_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "tpch"],
    compute_kind="MySQL",
    group_name="bronze_layer",
    partitions_def=None,
)
def dummy(context) -> Output[pd.DataFrame]:
    sql_stm = f"SELECT * FROM orders "
    try:
        partition_date_str = context.asset_partition_key_for_output()
        context.log.info(partition_date_str)
        # TODO: your code here, sql_stm query by partition?
        sql_stm += f"""WHERE DATE(O_ORDERDATE) = '{partition_date_str}'"""
    except Exception:
        context.log.info(f"Orders has no partition key! Yaya")
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    context.log.info(pd_data.columns)

    return Output(pd_data, metadata={"table": "Orders"})


@asset(
    name="silver_dummy2",
    io_manager_key="minduck_io_manager",
    key_prefix=["silver", "tpch"],
    ins={
        "bronze_dummy": AssetIn(
            key_prefix=["bronze", "tpch"],
        ),
    },
    compute_kind="DuckDB",
    group_name="silver_layer",
    partitions_def=None,
)
def dummy2(context, bronze_dummy) -> Output[str]:
    sql_stm = f"""
        with CTE as {bronze_dummy}
        SELECT * FROM CTE
    """

    return Output(sql_stm, metadata={"table": "dummy"})
