from contextlib import contextmanager

# from datetime import datetime
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine


@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        # tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"

        with connect_psql(self._config) as db_conn:
            # primary_keys = (context.metadata or {}).get("primary_keys", [])
            ls_columns = (context.metadata or {}).get("columns", [])
            obj[ls_columns].to_sql(
                name=table,
                con=db_conn,
                schema=schema,
                if_exists="replace",
                index=False,
                chunksize=10000,
                method="multi",
            )


'''
            with db_conn.connect() as cursor:
                # create temp table
                cursor.execute(
                    f"CREATE TEMP TABLE IF NOT EXISTS {tmp_tbl} (LIKE {schema}.{table})"
                )

                # insert new data
                obj[ls_columns].to_sql(
                    name=tmp_tbl,
                    con=db_conn,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                    method="multi",
                )

                with db_conn.connect() as cursor:
                    # check data inserted
                    result = cursor.execute(f"SELECT COUNT(*) FROM {tmp_tbl}")
                    for row in result:
                        print(f"Temp table records: {row}")

                    # upsert data
                    if len(primary_keys) > 0:
                        conditions = " AND ".join(
                            [
                                f""" {schema}.{table}."{k}" = {tmp_tbl}."{k}" """
                                for k in primary_keys
                            ]
                        )
                        command = f"""
                        BEGIN TRANSACTION;
                        DELETE FROM {schema}.{table}
                        USING {tmp_tbl}
                        WHERE {conditions};
                        INSERT INTO {schema}.{table}
                        SELECT * FROM {tmp_tbl};
                        END TRANSACTION;
                        """
                    else:
                        command = f"""
                        BEGIN TRANSACTION;
                        TRUNCATE TABLE {schema}.{table};
                        INSERT INTO {schema}.{table}
                        SELECT * FROM {tmp_tbl};
                        END TRANSACTION;
                        """

                    cursor.execute(command)

                    # drop temp table
                    cursor.execute(f"DROP TABLE IF EXISTS {tmp_tbl}")
'''
