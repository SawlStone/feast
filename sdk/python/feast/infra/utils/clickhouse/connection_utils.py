import contextlib
from typing import Dict

import numpy as np
import pandas as pd
import pyarrow as pa
from clickhouse_driver import Client

from feast.infra.utils.clickhouse.clickhouse_config import ClickHouseConfig
from feast.type_map import arrow_to_clickhouse_type


@contextlib.contextmanager
def clickhouse_connect(config):
    connection = Client(
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port,
        database=config.database,
    )
    try:
        yield connection
    except Exception as e:
        print(e)
    finally:
        # Close the connection
        connection.disconnect()


def _df_to_create_table_sql(entity_df, table_name) -> str:
    pa_table = pa.Table.from_pandas(entity_df)
    columns = [
        f""""{f.name}" {arrow_to_clickhouse_type(f.type)}""" for f in pa_table.schema
    ]
    return f'CREATE TABLE "{table_name}" ({", ".join(columns)}) engine = Memory;'


def df_to_clickhouse_table(
    config: ClickHouseConfig, df: pd.DataFrame, table_name: str
) -> Dict[str, np.dtype]:
    """
    Create a table for the data frame, insert all the values, and return the table schema
    """
    with clickhouse_connect(config) as client:
        client.execute(_df_to_create_table_sql(df, table_name))
        client.insert_dataframe(
            f"INSERT INTO {table_name} VALUES", df, settings={"use_numpy": True}
        )
    return dict(zip(df.columns, df.dtypes))
