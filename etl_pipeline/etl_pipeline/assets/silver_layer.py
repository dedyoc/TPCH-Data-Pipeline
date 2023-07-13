import pandas as pd
from dagster import asset, AssetIn, Output, DailyPartitionsDefinition
import duckdb


@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["intermediate", "tpch"],
    compute_kind="DuckDB",
    group_name="intermediate",
    ins={
        "bronze_nation": AssetIn(
            key_prefix=["bronze", "tpch"],
        ),
        "bronze_region": AssetIn(
            key_prefix=["bronze", "tpch"],
        ),
    },
)
def int_location(context, bronze_nation, bronze_region):
    duckdb.register("nation", bronze_nation)
    duckdb.register("region", bronze_region)

    # Run your SQL query using DuckDB
    result = duckdb.sql(
        """
        select
            nation.n_regionkey as regionkey,
            nation.n_nationkey as nationkey,
            region.r_name as continent,
            nation.n_name as country
        from
            nation
        inner join region
            on nation.n_regionkey = region.r_regionkey
    """
    )

    # Convert the result to a Pandas DataFrame
    result_df = result.to_df()
    context.log.info(result_df.dtypes)
    return Output(
        result_df,
        metadata={
            "table": "int_location",
            "records count": len(result_df),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "tpch"],
    compute_kind="DuckDB",
    group_name="silver_layer",
    ins={
        "bronze_customer": AssetIn(
            key_prefix=["bronze", "tpch"],
        ),
        "int_location": AssetIn(
            key_prefix=["intermediate", "tpch"],
        ),
    },
)
def dim_customers(context, bronze_customer, int_location):
    duckdb.register("customer", bronze_customer)
    duckdb.register("location", int_location)

    # Run your SQL query using DuckDB
    result = duckdb.sql(
        """
        select
            customer.c_custkey as customer_key,
            customer.c_name as customer_name,
            customer.c_address as customer_address,
            location.regionkey as customer_region_key,
            location.nationkey as customer_nation_key,
            location.country as customer_country,
            location.continent as customer_continent,
            customer.c_phone as customer_phone_number,
            customer.c_acctbal as customer_account_balance,
            customer.c_mktsegment as customer_market_segment
        from
            customer
            left join location
            on customer.c_nationkey = location.nationkey
        order by
        customer.c_custkey
    """
    )

    # Convert the result to a Pandas DataFrame
    result_df = result.to_df()
    context.log.info(result_df.dtypes)
    return Output(
        result_df,
        metadata={
            "table": "int_location",
            "records count": len(result_df),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "tpch"],
    compute_kind="DuckDB",
    group_name="silver_layer",
    ins={
        "bronze_part": AssetIn(
            key_prefix=["bronze", "tpch"],
        )
    },
)
def dim_parts(context, bronze_part):
    duckdb.register("parts", bronze_part)
    # Run your SQL query using DuckDB
    result = duckdb.sql(
        """
        select 
            parts.p_partkey as part_key,
            parts.p_name as part_name,
            parts.p_mfgr as part_manufacturer_name,
            parts.p_brand as part_brand,
            parts.p_type as part_type,
            parts.p_size as part_size,
            parts.p_container as part_container,
            parts.p_retailprice as part_retail_price
        from
            parts
        order by
            parts.p_partkey
    """
    )

    # Convert the result to a Pandas DataFrame
    result_df = result.to_df()
    context.log.info(result_df.dtypes)
    return Output(
        result_df,
        metadata={
            "table": "dim_parts",
            "records count": len(result_df),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "tpch"],
    compute_kind="DuckDB",
    group_name="silver_layer",
    ins={
        "bronze_supplier": AssetIn(
            key_prefix=["bronze", "tpch"],
        ),
        "int_location": AssetIn(
            key_prefix=["intermediate", "tpch"],
        ),
    },
)
def dim_suppliers(context, bronze_supplier, int_location):
    duckdb.register("suppliers", bronze_supplier)
    duckdb.register("location", int_location)
    # Run your SQL query using DuckDB
    result = duckdb.sql(
        """
        select 
            suppliers.s_suppkey as supplier_key,
            suppliers.s_name as supplier_name,
            suppliers.s_address as supplier_address,
            location.nationkey as supplier_nation_key,
            location.country as supplier_country,
            location.regionkey as supplier_region_key,
            location.continent as supplier_continent,
            suppliers.s_phone as supplier_phone_number,
            suppliers.s_acctbal as supplier_account_balance
        from
            suppliers
            join
            location
                on suppliers.s_nationkey = location.nationkey
        order by 
        suppliers.s_suppkey
    """
    )
    # Convert the result to a Pandas DataFrame
    result_df = result.to_df()
    context.log.info(result_df.dtypes)
    return Output(
        result_df,
        metadata={
            "table": "dim_suppliers",
            "records count": len(result_df),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "tpch"],
    compute_kind="DuckDB",
    group_name="silver_layer",
    ins={
        "bronze_orders": AssetIn(
            key_prefix=["bronze", "tpch"],
        ),
        "bronze_lineitem": AssetIn(
            key_prefix=["bronze", "tpch"],
        ),
    },
)
def fact_line_orders(context, bronze_lineitem, bronze_orders):
    duckdb.register("lineitem", bronze_lineitem)
    duckdb.register("orders", bronze_orders)
    # Run the query
    result = duckdb.query(
        """
        SELECT
            lineitem.l_orderkey || lineitem.l_linenumber as order_item_key,
            orders.o_orderkey,
            orders.o_custkey as order_customer_key,
            lineitem.l_partkey as part_key,
            lineitem.l_suppkey as supplier_key,
            lineitem.l_linenumber as line_number,
            orders.o_orderdate as order_date,
            lineitem.l_commitdate as commit_date,
            lineitem.l_shipdate as ship_date,
            lineitem.l_receiptdate as receipt_date,
            orders.o_orderpriority as order_priority,
            orders.o_shippriority as ship_priority,
            lineitem.l_returnflag as return_flag,
            lineitem.l_linestatus as line_status,
            lineitem.l_quantity as quantity,
            lineitem.l_extendedprice as marked_price,
            round(lineitem.l_discount * 100, 2) as discount_percentage,
            round(lineitem.l_tax * 100, 2) as tax_percentage,
            lineitem.l_extendedprice * lineitem.l_discount as discounted_price,
            lineitem.l_extendedprice * lineitem.l_tax as tax_amount,
            round(
                lineitem.l_extendedprice - (lineitem.l_extendedprice * lineitem.l_discount) + (
                    (
                        lineitem.l_extendedprice - (lineitem.l_extendedprice * lineitem.l_discount)
                    ) * lineitem.l_tax
                ),
                2
            ) as selling_price,
            orders.o_totalprice as order_total_price,
            lineitem.l_shipmode as ship_mode
        FROM
            orders
            INNER JOIN lineitem
            ON orders.o_orderkey = lineitem.l_orderkey
        ORDER BY
            orders.o_orderdate;
    """
    )
    # Convert the result to a Pandas DataFrame
    result_df = result.to_df()
    context.log.info(result_df.dtypes)
    return Output(
        result_df,
        metadata={
            "table": "fact_line_orders",
            "records count": len(result_df),
        },
    )
