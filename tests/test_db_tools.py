from unittest import TestCase, main
from unittest.mock import patch, MagicMock, call
import sqlite3
from tempfile import mkdtemp
import os
from pathlib import Path

import pandas as pd
import yaml

from src import db_tools


class TestIntegrationDBLocal(TestCase):
    def setUp(self):
        tempdir = mkdtemp()
        dbname = 'testdb'
        self.dbpath = Path(tempdir) / dbname
        self.cursor = sqlite3.connect(self.dbpath).cursor()
        config_path = Path(__file__).parent.parent / 'config' / "db" / "oryxloss_schema_rolling.yaml"
        with open(config_path) as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)

        self.tbl_name = "loss_item"

        # Create table cmd
        loss_tbl_sql = f"CREATE TABLE IF NOT EXISTS {self.tbl_name} ("
        col_defintions = []
        for col_name, col_config in self.config["tables"][self.tbl_name]["columns"].items():
            col_sql = f"{col_name} {col_config['type']}"
            if 'not_null' in col_config and col_config['not_null']:
                col_sql += " NOT NULL"
            if 'unique' in col_config and col_config['unique']:
                col_sql += " UNIQUE"
            if 'auto_inc_primary_key' in col_config and col_config['auto_inc_primary_key']:
                col_sql += " PRIMARY KEY AUTOINCREMENT"
            col_defintions.append(col_sql)
        loss_tbl_sql = f"CREATE TABLE IF NOT EXISTS {self.tbl_name} ({', '.join(col_defintions)})"

        print(loss_tbl_sql)

        # Creating table
        self.cursor.execute(loss_tbl_sql)

    def test_setup(self):
        # Query what tables are in database
        # If this fails, setup is not creating the expected table

        # Checking if table exists
        query = f"SELECT * FROM sqlite_master WHERE type='table' and name='{self.tbl_name}'"
        sqlite_master_result = self.cursor.execute(query).fetchall()
        self.assertEqual(len(sqlite_master_result), 1)

        # Checking if table is empty
        query_tbl = "SELECT * FROM loss_item"
        result = self.cursor.execute(query_tbl).fetchall()
        self.assertEqual(len(result), 0)

        # Checking if columns are as expected
        columns = [description[0] for description in self.cursor.description] if self.cursor.description else []
        expected_columns = ['start_date', 'stop_date', 'conflict', 'party', 'category_name', 'type_name', 'loss_id', 'loss_type', 'proof_id']
        self.assertEqual(columns, expected_columns)
        query = f"PRAGMA table_info({self.tbl_name})"
        result = self.cursor.execute(query).fetchall()
        self.assertEqual(len(result), len(expected_columns))


# class TestDBConn(TestCase):
#
#     def setUp(self):
#         setup_path = "Some/path/to/db.db"
#         self.dbconn = db_tools.DBConn(setup_path)
#         self.dbconn.conn = MagicMock()
#         self.dbconn.cursor = MagicMock()
#
#     # @patch("src.db_tools.sqlite3")
#     def test_scd_type_two_query(self):
#         test_data = {
#             'start_date': ['2023-01-01', '2023-05-15', '2024-03-20'],
#             'stop_date': ['2023-02-01', '2023-06-15', '2024-04-20'],
#             'conflict': ['Conflict A', 'Conflict B', 'Conflict C'],
#             'party': ['Party X', 'Party Y', 'Party Z'],
#             'category_name': ['Category 1', 'Category 2', 'Category 3'],
#             'type_name': ['Type Alpha', 'Type Beta', 'Type Gamma'],
#             'loss_id': [101, 102, 103],
#             'loss_type': ['Destroyed', 'Damaged', 'Abandoned'],
#             'proof_id': [111, 222, 333],
#         }
#         fake_pandas_df = pd.DataFrame(test_data)
#         print(fake_pandas_df.to_string())
#
#         sql = self.dbconn._scd_type_two_query(fake_pandas_df, "my_table", "2025-07-04")
#         print(sql)
#
#         sql2 = self.dbconn._sql_column_filters(fake_pandas_df, ["stop_date"])
#         print(sql2)






if __name__ == '__main__':
    main()
