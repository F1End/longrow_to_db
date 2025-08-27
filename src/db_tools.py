"""
Classes/functions for database interaction
"""
from typing import Union, Optional, Any
from pathlib import Path
from collections.abc import Iterable
import sqlite3
import logging
from datetime import date

import pandas as pd
from pyspark.sql import dataframe

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
        results = self.conn.execute(sql_safe, data).fetchall()
        return results

    def push_or_ignore(self, sql: str, data: Iterable) -> None:
        logger.debug(f"Running query: {sql}")
        logger.debug(f"Query items: {data}")
        self.conn.executemany(sql, data)

    def _simple_query(self, sql) -> Any:
        logger.debug(f"Running query: {sql}")
        results = self.cursor.execute(sql)
        return results.fetchall()

    def append_db(self, df, table_name: str) -> None:
        if isinstance(df, dataframe.DataFrame):
            pandas_df = df.toPandas()
        elif isinstance(df, pd.DataFrame):
            pandas_df = df
        else:
            raise TypeError("df must be DataFrame or Pandas DataFrame!")
        values = [tuple(row) for row in pandas_df.itertuples(index=False, name=None)]
        sql = self._insert_or_ignore_sql(pandas_df, table_name)
        self.push_or_ignore(sql, values)
        logger.info(f"Updated data in table {table_name} with {len(pandas_df)} items.")

    def roll_db_data(self, pyspark_df, table_name: str):
        pandas_df = pyspark_df.toPandas()

    def append_db_scd_type_two(self, pyspark_df, table_name: str) -> None:
        temp_table_name = self._create_temp_table(table_name)
        temp_data = self._format_df_for_temp_storage(pyspark_df)
        self.append_db(temp_data, temp_table_name)
        # sql = self._scd_type_two_query(table_name, temp_table_name, pyspark_df.columns)

    def _format_df_for_temp_storage(self, pyspark_df: dataframe) -> None:
        pandas_df = pyspark_df.toPandas()
        if "start_date" not in pandas_df.columns \
        and "stop_date" not in pandas_df.columns \
        and "as_of" in pandas_df.columns:
            pandas_df["start_date"] = pandas_df["as_of"]
            pandas_df["stop_date"] = pandas_df["as_of"]
            pandas_df = pandas_df.drop("as_of", axis=1)
            pandas_df = pandas_df[["start_date", "stop_date"] + [col for col in pandas_df.columns if col not in ["start_date","stop_date"]]]
        else:
            logger.warning("Skipping temp df formatting as it does not follow structure required for as_of -> start_date/stop_date conversion.")
        return pandas_df



    def _scd_type_two_query(self, table_name: str, temp_table_name: str, columns: list[str],
                            as_of: Union[str, date]) -> None:
        sql = (self._sql_scd_close_outdated(table_name, temp_table_name, columns) +
               self._sql_scd_insert_new(table_name, temp_table_name, columns)
               )
        return sql

    def _sql_scd_close_outdated(self, table_name: str, temp_table_name: str, columns: list[str]) -> str:
        sql = f"""
        SELECT * FROM loss_item_2 s2
        WHERE NOT EXISTS (
        SELECT 1
        FROM loss_item s1
        WHERE s1.conflict = s2.conflict
        AND s1.party = s2.party
        AND s1.category_name = S2.category_name
        AND s1.type_name = s2.type_name
        AND s1.loss_id = s2.loss_id
        AND s1.loss_type = s2.loss_type
        AND s1.proof_id = s2.proof_id
        )
        """
        return sql

    def _sql_scd_insert_new(self, table_name: str, temp_table_name: str, columns: list[str]) -> str:
        sql = f"""
        INSERT INTO ?
        SELECT as_of as start_date, "2222-12-31" as stop_date, s1.conflict, s1.party, s1.category_name,
        s1.type_name, s1.loss_id, s1.loss_type, s1.proof_id 
        FROM ? s1
        WHERE NOT EXISTS (
        SELECT 1
        FROM ? s2
        WHERE s1.conflict = s2.conflict
        AND s1.party = s2.party
        AND s1.category_name = S2.category_name
        AND s1.type_name = s2.type_name
        AND s1.loss_id = s2.loss_id
        AND s1.loss_type = s2.loss_type
        )
        """
        arguments = [table_name, temp_table_name, table_name]
        return sql, arguments

    def _create_temp_table(self, table_name: str) -> str:
        temp_table_name = "temp_" + table_name
        logger.info(f"Creating TEMPORARY table {temp_table_name} if does not exist")
        cmd = self._temp_table_cmd(table_name)
        logger.debug(f"Running command:\n {cmd}")
        self.cursor.execute(cmd)
        return temp_table_name

    def _sql_temp_tbl_scd_type_two_query(self, pandas_df, table_name: str, as_of: Union[str, date]) -> str:
        pass

    def _sql_column_filters(self, pandas_df: pd.DataFrame, cols_to_exclude: Iterable) -> pd.DataFrame:
        cols = [col for col in list(pandas_df.columns) if col not in cols_to_exclude]
        delimiter = " = ? AND "
        sql_col_names = " = ? AND ".join(cols) + " = ?"
        return sql_col_names

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

    def _temp_table_cmd(self, table_name, convert_scd: Optional[bool] = True,
                        temp_tbl_name: Optional[str] = None) -> str:
        tbl_info_sql = f"""SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?;"""
        origin_tbl_cmd = self.run_query(tbl_info_sql, [table_name])[0]
        temp_tbl_cmd = origin_tbl_cmd[0].replace(f"CREATE TABLE ", "CREATE TEMP TABLE")
        if temp_tbl_name:
            temp_tbl_cmd = temp_tbl_cmd.replace(table_name, temp_tbl_name)
        else:
            temp_tbl_cmd = temp_tbl_cmd.replace(table_name, f"temp_{table_name}")
        if convert_scd:
            temp_tbl_cmd.replace("start_date", "as_of")
            temp_tbl_cmd.replace("end_date TEXT", "")
        return temp_tbl_cmd

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
