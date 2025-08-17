# In[1]: Import Packages

from typing import Any, List, Optional, Tuple

import numpy as np
import pandas as pd
from trino import dbapi
from trino.auth import BasicAuthentication

from Configurations import db_config as config

from . import QueriesLoader

# In[2]: Create Class for TrinoClient with Multiple Methods [Create Table - Get Tables - TrinoClient Connection - Insert in Tables]


class TrinoClient:
    def __init__(self) -> None:
        self._connection = None
        self._columns = None
        self._sql_query = None
        self._dB = config.database

    def connect_to_trino(self) -> dbapi.Connection:
        try:
            # Optional auth (only if you actually have a password configured)
            auth = None
            if self._dB.get("password"):
                auth = BasicAuthentication(self._dB["user"], self._dB["password"])

            # Establishing a connection to the TrinoClient server
            self._connection = dbapi.connect(
                host=self._dB["host"],
                port=int(self._dB.get("port", 8080)),
                user=self._dB["user"],
                catalog=self._dB.get("catalog"),
                schema=self._dB.get("schema"),
                http_scheme=self._dB.get("http_scheme", "http"),
                auth=auth,
            )
            print("Connected to Trino.")
            return self._connection

        except Exception as e:
            self._connection = None
            raise RuntimeError(f"Failed to connect to Trino: {e}") from e

    def execute_operation_by_name(self, task_name: Optional[str] = None) -> None:
        objql = QueriesLoader.QueriesLoader("trino_queries")
        queries = objql.safe_load()

        # Get the specific configuration by name
        entry = objql.get_config_by_name(queries, task_name)
        table = entry["table"]
        columns = entry["columns"]
        try:
            column_names = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            _sql_query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
        except:
            columns, column_names, placeholders = None, None, None
            _sql_query = entry["query"]

        self._columns = columns
        self._sql_query = _sql_query

        print(self._sql_query)
        return None

    def run_query(self) -> List[Tuple[Any]]:
        if self._connection is None:
            print("No connection established. Please call connect_to_trino() first.")
            return None

        try:
            cursor = self._connection.cursor()
            cursor.execute(self._sql_query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {e}") from e

    def get_db_tables(self, query: str, batch_size: int = 1000000) -> pd.DataFrame:
        if self._connection is None:
            print("No connection established. Please call connect_to_trino() first.")
            return None

        try:
            cur = self._connection.cursor()
            cur.execute(query)

            rows = []
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                rows.extend(batch)

            # Convert the result to a DataFrame
            columns = [col[0] for col in cur.description]
            df = pd.DataFrame(rows, columns=columns)

            # Check if DataFrame is empty
            if df.empty:
                print("Table exists but is empty.")
                return None

            return df

        except Exception as e:
            print(f"Error executing Trino query: {e}")

        return None

    def insert_dataframe_to_trino(self, df: pd.DataFrame = None) -> None:
        try:
            # Convert DataFrame to list of tuples
            df = df.replace({np.nan: None})
            data = [tuple(row) for row in df[self._columns].values]

            with self._connection.cursor() as cursor:
                cursor.executemany(self._sql_query, data)

            self._connection.commit()
            print("DataFrame inserted into Trino table successfully!")
            return True
        except Exception as e:
            print(f"Error inserting DataFrame into Trino: {e}")
        finally:
            self._connection.close()

    def terminate_connection(self) -> None:
        try:
            if self._connection is not None:
                self._connection.close()
                print("Connection has been terminated.")
            else:
                print("No active connection to terminate.")
        finally:
            self._connection = None
