"""
Classes/functions for database interaction
"""
from typing import Union, Optional, Any
from pathlib import Path
from collections.abc import Iterable
import sqlite3
import logging


logger = logging.getLogger(__name__)


class DBConn:
    def __init__(self, db_path: Union[Path, str]):
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        logger.debug(f"Opened connection to {self.db_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cursor.close()
        self.conn.close()
        logger.debug(f"Closed connection to {self.db_path}")

    def run_query(self, sql_safe: str, data: Iterable) -> Any:
        logger.debug(f"Running query: {sql_safe}")
        logger.debug(f"Query items: {data}")
        results = self.cursor.execute(sql_safe, data).fetchall()
        return results

    def push_or_ignore(self, sql: str, data: Iterable):
        logger.debug(f"Running query: {sql}")
        logger.debug(f"Query items: {data}")
        self.conn.executemany(sql, data)

    def _simple_query(self, sql):
        logger.debug(f"Running query: {sql}")
        results = self.cursor.execute(sql)
        return results.fetchall()

    def append_db(self, pyspark_df, table_name: str):
        pandas_df = pyspark_df.toPandas()
        values = [tuple(row) for row in pandas_df.itertuples(index=False, name=None)]
        sql = self._insert_or_ignore_sql(pandas_df, table_name)
        self.push_or_ignore(sql, values)
        logger.info(f"Updated data in table {table_name} with {len(pandas_df)} items.")

    def _insert_or_ignore_sql(self, pandas_df, table_name: str) -> str:
        cols = list(pandas_df.columns)
        placeholders = ",".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        sql = f"INSERT OR IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})"
        return sql

    def fetch_unique_data(self, spark_df, spark_df_col_nane, db_table_name, db_col_name):
        data_list = self._spark_col_to_list(spark_df, spark_df_col_nane)
        placeholders = ",".join(["?"] * len(data_list))
        sql = f"SELECT * FROM {db_table_name} WHERE {db_col_name} in ({placeholders})"
        logger.debug(f"Parsed item count: {len(placeholders)}")
        result = self.run_query(sql, data_list)
        return result

    def _spark_col_to_list(self, pyspark_df, col_name: str) -> list:
        converted_val = [row[col_name] for row in pyspark_df.select(col_name).distinct().collect()]
        return converted_val

    def build_db(self, db_schema: dict):
        logger.warning(f"Initializing Database at {self.db_path}")
        for table_name, table_config in db_schema["tables"].items():
            self.create_table(table_name, table_config)
        if "indexes" in db_schema:
            for index_name, index_confing in db_schema["indexes"].items():
                self.create_index(index_name, index_confing)

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        logger.debug(f"Connected to {self.db_path}")

    def create_table(self, table_name, table_config):
        logger.info(f"Creating table {table_name} if does not exist")
        cmd = self._table_cmd(table_name, table_config)
        logger.debug(f"Running command:\n {cmd}")
        self.cursor.execute(cmd)

    def create_index(self, index_name, index_config):
        logger.info(f"Creating index {index_name} if does not exist")
        cmd = self._index_cmd(index_name, index_config)
        logger.debug(f"Running command:\n {cmd}")

    def _index_cmd(self, index_name, index_config: dict):
        table_name = index_config["table"]
        columns = ", ".join(index_config['columns'])
        cmd = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns})"
        return cmd

    def _table_cmd(self, table_name, table_config):
        table_def = self._table_definition(table_config)
        cmd = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(table_def)})"
        return cmd

    def _table_definition(self, table_config: dict) -> list:
        columns = self._col_definitions(table_config)
        constraints = self._table_constraints(table_config)
        return columns + constraints

    def _col_definitions(self, table_config: dict) -> list:
        columns = []
        for col_name, col_config in table_config["columns"].items():
            col_definition = f"{col_name} {col_config['type']}" + self._col_constraints(col_config)
            columns.append(col_definition)
        return columns

    def _col_constraints(self, col_config):
        col_constraints = ""
        if 'not_null' in col_config and col_config['not_null']:
            col_constraints += " NOT NULL"

        if 'unique' in col_config and col_config['unique']:
            col_constraints += " UNIQUE"

        if 'auto_inc_primary_key' in col_config and col_config['auto_inc_primary_key']:
            col_constraints += " PRIMARY KEY AUTOINCREMENT"

        return col_constraints

    def _primary_key_constraits(self, table_config):
        if isinstance(table_config['primary_key'], list):
            pk_cols = ", ".join(table_config['primary_key'])
            return f"PRIMARY KEY ({pk_cols})"
        else:
            return f"PRIMARY KEY ({table_config['primary_key']})"

    def _foreign_key_constraints(self, table_config):
        foreign_keys = []
        for key in table_config['foreign_keys']:
            key_cmd = f"FOREIGN KEY ({key['column']}) REFERENCES {key['references_table']}({key['references_column']})"
            foreign_keys.append(key_cmd)
        return foreign_keys

    def _table_constraints(self, table_config):
        constraints = []
        if 'primary_key' in table_config:
            constraints.append(self._primary_key_constraits(table_config))
        if 'foreign_keys' in table_config:
            constraints += self._foreign_key_constraints(table_config)

        return constraints
