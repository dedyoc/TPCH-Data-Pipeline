import pandas as pd
from dagster import asset, AssetIn, AssetOut, Output, multi_asset

import duckdb


@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "tpch"],
    compute_kind="MinIO",
    group_name="gold_layer",
    ins={
        "dim_suppliers": AssetIn(
            key_prefix=["silver", "tpch"],
        ),
        "dim_parts": AssetIn(
            key_prefix=["silver", "tpch"],
        ),
        "fact_line_orders": AssetIn(
            key_prefix=["silver", "tpch"],
        ),
    },
)
def gold_market_share(
    context,
    dim_suppliers: pd.DataFrame,
    fact_line_orders: pd.DataFrame,
    dim_parts: pd.DataFrame,
):
    duckdb.register("suppliers", dim_suppliers)
    duckdb.register("fact_table", fact_line_orders)
    duckdb.register("parts", dim_parts)
    result = duckdb.query(
        """
        with order_with_dates_and_supp as (
            select
                fact_table.*,
                year(fact_table.order_date) as year,
                suppliers.*,
                parts.part_name
            from
                fact_table
                inner join suppliers
                on fact_table.supplier_key = suppliers.supplier_key
                inner join parts
                on parts.part_key = fact_table.part_key
        ),
        volume_per_country_year_part as (
            select
                supplier_country,
                year,
                part_key,
                sum(selling_price * quantity) as total_volume
            from
                order_with_dates_and_supp
            group by
                supplier_country,
                year,
                part_key
        ),
        volume_per_year_part as (
            select
                year,
                part_key,
                part_name,
                sum(selling_price * quantity) as total_part_volume
            from
                order_with_dates_and_supp
            group by
                year,
                part_key,
                part_name
        ),
        final as (
            select
                volume_per_year_part.year,
                volume_per_year_part.part_key,
                volume_per_year_part.part_name,
                supplier_country,
                total_volume,
                total_part_volume,
                (
                    total_volume / total_part_volume
                ) * 100 as total_market_share
            from
                volume_per_year_part
                inner join volume_per_country_year_part
                on volume_per_year_part.part_key = volume_per_country_year_part.part_key
                and volume_per_year_part.year = volume_per_country_year_part.year
        )
        select
            *
        from
            final
    """
    )
    result_df = result.to_df()
    context.log.info(result_df.columns)

    return Output(
        result_df,
        metadata={
            "table": "gold_market_share",
            "records count": len(result_df),
        },
    )


@multi_asset(
    ins={
        "gold_market_share": AssetIn(
            key_prefix=["gold", "tpch"],
        )
    },
    outs={
        "market_share": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "gold"],
            metadata={
                "columns": [
                    "year",
                    "part_key",
                    "part_name",
                    "supplier_country",
                    "total_volume",
                    "total_part_volume",
                    "total_market_share",
                ],
            },
            group_name="warehouse_layer",
        )
    },
    compute_kind="PostgreSQL",
)
def market_share(gold_market_share):
    return Output(
        gold_market_share,
        metadata={
            "schema": "gold",
            "table": "market_share",
            "records count": len(gold_market_share),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "tpch"],
    compute_kind="MinIO",
    group_name="gold_layer",
    ins={
        "dim_parts": AssetIn(
            key_prefix=["silver", "tpch"],
        ),
        "dim_customers": AssetIn(
            key_prefix=["silver", "tpch"],
        ),
        "fact_line_orders": AssetIn(
            key_prefix=["silver", "tpch"],
        ),
    },
)
def gold_return_orders(
    context,
    dim_customers: pd.DataFrame,
    dim_parts: pd.DataFrame,
    fact_line_orders: pd.DataFrame,
):
    duckdb.register("customers", dim_customers)
    duckdb.register("parts", dim_parts)

    duckdb.register("fact_table", fact_line_orders)

    result = duckdb.query(
        """
        select
            fact_table.*,
            customers.customer_name,
            customers.customer_address,
            customers.customer_country,
            customers.customer_continent,
            customers.customer_account_balance,
            customers.customer_market_segment,
            parts.part_name,
            parts.part_manufacturer_name,
            parts.part_brand,
            parts.part_type,
            parts.part_size
        from
            fact_table
            inner join customers
            on fact_table.order_customer_key = customers.customer_key
            inner join parts
            on fact_table.part_key = parts.part_key
        where
            return_flag = 'R'
    """
    )
    result_df = result.to_df()
    context.log.info(result_df.columns)

    return Output(
        result_df,
        metadata={
            "table": "gold_market_share",
            "records count": len(result_df),
        },
    )
