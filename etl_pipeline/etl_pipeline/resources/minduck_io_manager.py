import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import duckdb


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        raise


class DuckDB:
    def __init__(self, config):
        self.config = config

    def query(self, select_statement, parquet_file_path: str):
        db = duckdb.connect(":memory:")
        db.query("install httpfs; load httpfs;")
        db.query(
            f"""
        set s3_access_key_id='minio';
        set s3_secret_access_key='minio123';
        set s3_endpoint='minio:9000';
        set s3_use_ssl='false';
        set s3_url_style='path';
        """
        )

        # Check if select_statement is a pandas DataFrame
        if isinstance(select_statement, pd.DataFrame):
            # Register DataFrame as DuckDB in-memory table
            db.register("df", select_statement)

            # Write in-memory table to Parquet file
            db.query(
                f"COPY df TO '{parquet_file_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY')"
            )
        else:
            # Execute SQL query and write results to a Parquet file
            db.query(
                f"COPY ({select_statement}) TO '{parquet_file_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY')"
            )


@contextmanager
def connect_duckdb(config):
    client = DuckDB(config)
    try:
        yield client
    except Exception:
        raise


class MinDuckIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        if context.has_asset_partitions:
            # TODO: your code here
            start, end = context.asset_partitions_time_window
            key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
            dt_format = "%Y%m%d%H%M%S"
            partition_str = start.strftime(dt_format)
            return os.path.join(key, f"{partition_str}.pq")
        else:
            return f"{key}.pq"

    def handle_output(self, context: OutputContext, select_statement):
        # convert to parquet format
        key_name = "s3://" + self._config.get("bucket") + "/" + self._get_path(context)

        # upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                # Make bucket if not exist.

                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")
            with connect_duckdb(self._config) as client:
                context.log.info(select_statement + " " + key_name)
                client.query(select_statement, key_name)
        except Exception:
            raise

    def load_input(self, context: InputContext):
        bucket_name = self._config.get("bucket")
        key_name = self._get_path(context)
        try:
            with connect_minio(self._config) as client:
                # Make bucket if not exist.
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")
                url = f"(select * from read_parquet('s3://{bucket_name}/{key_name}'))"
                return url
        except Exception:
            raise
