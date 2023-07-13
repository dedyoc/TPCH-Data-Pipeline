from .bronze_layer import dummy, extract_dataset_tpch, dummy2
from dagster import DailyPartitionsDefinition, load_assets_from_modules
from .silver_layer import (
    int_location,
    dim_customers,
    dim_parts,
    dim_suppliers,
    fact_line_orders,
)
from .gold_layer import (
    gold_market_share,
    market_share,
)


ls_metadatas_tpch = {
    "part": {
        "partition": None,
        "columns": [
            "P_PARTKEY",
            "P_NAME",
            "P_MFGR",
            "P_BRAND",
            "P_TYPE",
            "P_SIZE",
            "P_CONTAINER",
            "P_RETAILPRICE",
            "P_COMMENT",
        ],
    },
    "supplier": {
        "partition": None,
        "columns": [
            "S_SUPPKEY",
            "S_NAME",
            "S_ADDRESS",
            "S_NATIONKEY",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
        ],
    },
    "partsupp": {
        "partition": None,
        "columns": [
            "PS_PARTKEY",
            "PS_SUPPKEY",
            "PS_AVAILQTY",
            "PS_SUPPLYCOST",
            "PS_COMMENT",
        ],
    },
    "customer": {
        "partition": None,
        "columns": [
            "C_CUSTKEY",
            "C_NAME",
            "C_ADDRESS",
            "C_NATIONKEY",
            "C_PHONE",
            "C_ACCTBAL",
            "C_MKTSEGMENT",
            "C_COMMENT",
        ],
    },
    "orders": {
        "partition": None,  # DailyPartitionsDefinition(start_date="1995-01-01"),
        "columns": [
            "O_ORDERKEY",
            "O_CUSTKEY",
            "O_ORDERSTATUS",
            "O_TOTALPRICE",
            "O_ORDERDATE",
            "O_ORDERPRIORITY",
            "O_CLERK",
            "O_SHIPPRIORITY",
            "O_COMMENT",
        ],
    },
    "lineitem": {
        "partition": None,
        "columns": [
            "L_ORDERKEY",
            "L_PARTKEY",
            "L_SUPPKEY",
            "L_LINENUMBER",
            "L_QUANTITY",
            "L_EXTENDEDPRICE",
            "L_DISCOUNT",
            "L_TAX",
            "L_RETURNFLAG",
            "L_LINESTATUS",
            "L_SHIPDATE",
            "L_COMMITDATE",
            "L_RECEIPTDATE",
            "L_SHIPINSTRUCT",
            "L_SHIPMODE",
            "L_COMMENT",
        ],
    },
    "nation": {
        "partition": None,
        "columns": [
            "N_NATIONKEY",
            "N_NAME",
            "N_REGIONKEY",
            "N_COMMENT",
        ],
    },
    "region": {
        "partition": None,
        "columns": [
            "R_REGIONKEY",
            "R_NAME",
            "R_COMMENT",
        ],
    },
}


assets = (
    #    [extract_dataset(table, ls_metadatas[table]["partition"]) for table in ls_metadatas]
    [
        extract_dataset_tpch(table, ls_metadatas_tpch[table]["partition"])
        for table in ls_metadatas_tpch
    ]
    + [
        dummy,
        dummy2,
        int_location,
        dim_customers,
        dim_parts,
        dim_suppliers,
        fact_line_orders,
        gold_market_share,
        market_share,
    ]
)
