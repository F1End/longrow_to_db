"""

"""
from typing import Union, Optional
from pathlib import Path
import sqlite3
import logging


logger = logging.getLogger(__name__)


class BuildDB:
    def __init__(self, path: Union[Path, str], schema: dict):
        self.db_path = path
        self.db_schema = schema
        self.conn = None
        self.cursor = None

    def run(self):
        logger.warning(f"Initializing Database at {self.db_path}")
        self.connect()
        for table_name, table_config in self.db_schema["tables"].items():
            self.create_table(table_name, table_config)
        if "indexes" in self.db_schema:
            for index_name, index_confing in self.db_schema["indexes"].items():
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


def to_sqlite(data, table_name: str, db_path, mode: str = "append"):
    """
    Saves the PySpark DataFrame (self.data) to an SQLite database file.

    :param table_name: The name of the table to save the data into.
    :param mode: Write mode for SQLite. Can be "replace", "append", or "fail".
    """
    # Convert PySpark DataFrame to Pandas DataFrame

    logger.debug(f"Converting to pandas")
    pandas_df = data.toPandas()
    print("******")
    print(f"LEN OF DF: {len(pandas_df)}")
    print(f"Sample:")
    print(pandas_df)
    print(pandas_df.head().to_string())
    print("******")

    # Establish SQLite connection
    conn = sqlite3.connect(db_path)

    # Write data to SQLite table
    logger.debug(f"Inserting data...")
    pandas_df.to_sql(table_name, conn, if_exists=mode, index=False)

    # Close the connection
    conn.close()
