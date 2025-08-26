from unittest import TestCase, main
from unittest.mock import patch, MagicMock, call
import sqlite3
from tempfile import mkdtemp
import os
from pathlib import Path
import logging

import pandas as pd
import yaml

from src import db_tools


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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

    def test_create_temp_table(self):
        table_name = self.tbl_name
        with db_tools.DBConn(self.dbpath) as conn:
            conn._create_temp_table(table_name)

            # Checking if table is empty
            query_tbl = f"SELECT * FROM temp_{self.tbl_name}"
            result = conn.cursor.execute(query_tbl).fetchall()
            self.assertEqual(len(result), 0)

            # Checking if columns are as expected
            columns = [description[0] for description in conn.cursor.description] if conn.cursor.description else []
            expected_columns = ['start_date', 'stop_date', 'conflict', 'party', 'category_name', 'type_name', 'loss_id', 'loss_type', 'proof_id']
            self.assertEqual(columns, expected_columns)
            query = f"PRAGMA table_info(temp_{self.tbl_name})"
            result = conn.cursor.execute(query).fetchall()
            self.assertEqual(len(result), len(expected_columns))

    def test_format_df_for_temp_storage(self):
        data_path = Path("resource") / Path("loss_input_1.csv")
        with open(data_path) as f:
            data = pd.read_csv(f)
        sparkdf_mock = MagicMock()
        sparkdf_mock.toPandas.return_value = data
        expected_df = data.copy()
        expected_df["start_date"] = expected_df["as_of"]
        expected_df["stop_date"] = expected_df["as_of"]
        expected_df = expected_df.drop("as_of", axis=1)
        expected_df = expected_df[
            ["start_date", "stop_date"] + [col for col in expected_df.columns if col not in ["start_date", "stop_date"]]]
        with db_tools.DBConn(self.dbpath) as conn:
            formatted_df = conn._format_df_for_temp_storage(sparkdf_mock)
            pd.testing.assert_frame_equal(formatted_df, expected_df)

    # Testing appending data to temp_table
    def test_append_db_scd_type_two(self):
        # Setting up data and mock
        data_path = Path("resource") / Path("loss_input_1.csv")
        with open(data_path) as f:
            data = pd.read_csv(f)
        sparkdf_mock = MagicMock()
        sparkdf_mock.toPandas.return_value = data
        table_name = self.tbl_name

        # Calling functions
        with db_tools.DBConn(self.dbpath) as conn:
            conn.append_db_scd_type_two(sparkdf_mock, table_name)

            # Checking if table has the data we wanted to push
            query_tbl = f"SELECT * FROM temp_{self.tbl_name}"
            result_df = pd.read_sql_query(query_tbl, conn.conn)
            # result = conn.cursor.execute(query_tbl).fetchall()
            self.assertEqual(len(result_df), len(data))
            for col in result_df.columns:
                if col in ["start_date", "stop_date"]:
                    pd.testing.assert_series_equal(result_df[col], data["as_of"], check_names=False)
                else:
                    pd.testing.assert_series_equal(result_df[col], data[col], check_names=True)



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
