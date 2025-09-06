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
        # setup general permanent data table
        tempdir = mkdtemp()
        dbname = 'testdb'
        self.dbpath = Path(tempdir) / dbname
        self.conn = sqlite3.connect(self.dbpath)
        self.cursor = self.conn.cursor()
        config_path = Path(__file__).parent.parent.parent / 'config' / "db" / "oryxloss_schema_rolling.yaml"
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

        # adding data
        self.base_data_path = Path("../data") / Path("loss_input_2_basic_loss_item.csv")
        with open(self.base_data_path) as f:
            self.data = pd.read_csv(f)
        self.data.to_sql(self.tbl_name, self.conn, if_exists="replace", index=False)


        # reading in temp table data and saving as formatted version for simpler testing downstream
        temp_data_path = Path("../data") / Path("loss_input_1.csv")
        with open(temp_data_path) as f:
            temp_data = pd.read_csv(f)
        temp_data["start_date"] = temp_data["as_of"]
        temp_data["stop_date"] = temp_data["as_of"]
        temp_data = temp_data.drop("as_of", axis=1)
        temp_data = temp_data[
            ["start_date", "stop_date"] + [col for col in temp_data.columns if col not in ["start_date", "stop_date"]]]
        self.temp_data = temp_data

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
        self.assertEqual(len(result), len(self.data))

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
        data_path = Path("../data") / Path("loss_input_1.csv")
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

    def test__sql_scd_insert_new(self):
        sql_filters = """temp_loss_item.conflict = loss_item.conflict AND temp_loss_item.party = loss_item.party AND temp_loss_item.category_name = loss_item.category_name AND temp_loss_item.type_name = loss_item.type_name AND temp_loss_item.loss_id = loss_item.loss_id AND temp_loss_item.loss_type = loss_item.loss_type AND temp_loss_item.proof_id = loss_item.proof_id"""
        instance = db_tools.DBConn(self.dbpath)
        sql = instance._sql_scd_insert_new(self.tbl_name, f"temp_{self.tbl_name}", sql_filters)
        expected = """
        INSERT INTO loss_item
        SELECT start_date, "2222-12-31" as stop_date, s1.conflict, s1.party, s1.category_name, s1.type_name, s1.loss_id, s1.loss_type, s1.proof_id
        FROM temp_loss_item s1
        WHERE NOT EXISTS (
        SELECT 1
        FROM loss_item s2
        WHERE s1.conflict = s2.conflict AND s1.party = s2.party AND s1.category_name = s2.category_name AND s1.type_name = s2.type_name AND s1.loss_id = s2.loss_id AND s1.loss_type = s2.loss_type AND s1.proof_id = s2.proof_id
        )
        """
        self.maxDiff = None
        self.assertEqual(sql, expected)

    def test__sql_scd_close_outdated(self):
        sql_filters = """temp_loss_item.conflict = loss_item.conflict AND temp_loss_item.party = loss_item.party AND temp_loss_item.category_name = loss_item.category_name AND temp_loss_item.type_name = loss_item.type_name AND temp_loss_item.loss_id = loss_item.loss_id AND temp_loss_item.loss_type = loss_item.loss_type AND temp_loss_item.proof_id = loss_item.proof_id"""
        instance = db_tools.DBConn(self.dbpath)
        sql = instance._sql_scd_close_outdated(self.tbl_name, f"temp_{self.tbl_name}", sql_filters)
        expected = """
        UPDATE loss_item
        SET stop_date = ?
        WHERE NOT EXISTS (
        SELECT 1
        FROM temp_loss_item
        WHERE temp_loss_item.conflict = loss_item.conflict AND temp_loss_item.party = loss_item.party AND temp_loss_item.category_name = loss_item.category_name AND temp_loss_item.type_name = loss_item.type_name AND temp_loss_item.loss_id = loss_item.loss_id AND temp_loss_item.loss_type = loss_item.loss_type AND temp_loss_item.proof_id = loss_item.proof_id
        )
        """
        self.assertEqual(sql, expected)

    def test_execute_scd_update(self):
        table_name = self.tbl_name
        temp_data = self.temp_data

        expected_data_path = Path("../data") / Path("loss_input_3_expected_loss_item.csv")
        with open(expected_data_path) as f:
            expected_df = pd.read_csv(f)

        temp_table_name = f"temp_{table_name}"
        temp_tbl_sql = f"""CREATE TEMP TABLE "{temp_table_name}" (
                          "start_date" TEXT,
                          "stop_date" TEXT,
                          "conflict" TEXT,
                          "party" TEXT,
                          "category_name" TEXT,
                          "type_name" TEXT,
                          "loss_id" INTEGER,
                          "loss_type" TEXT,
                          "proof_id" INTEGER
                          )"""

        # create connector (temp table exists only withint he same session)
        with db_tools.DBConn(self.dbpath) as conn:
            # create temp table and load data
            conn.cursor.execute(temp_tbl_sql)
            temp_data.to_sql(temp_table_name, conn.conn, if_exists="replace", index=False)

            # test if temp loading was successfully
            query_tbl = f"SELECT * FROM {temp_table_name}"
            result_df = pd.read_sql_query(query_tbl, conn.conn)
            self.assertEqual(len(result_df), len(temp_data))
            pd.testing.assert_frame_equal(result_df, temp_data)

            # The actual test
            conn._execute_scd_update(table_name, temp_table_name, "2025-03-31")
            updated_tbl_query = f"SELECT * FROM {table_name}"
            result_final_df = pd.read_sql_query(updated_tbl_query, conn.conn)
            pd.testing.assert_frame_equal(result_final_df, expected_df)

    # Testing appending data to temp_table
    def test_append_db_scd_type_two(self):
        # Setting up data and mock to create temp table
        sparkdf_mock = MagicMock()
        sparkdf_mock.toPandas.return_value = self.temp_data
        table_name = self.tbl_name

        expected_data_path = Path("../data") / Path("loss_input_3_expected_loss_item.csv")
        with open(expected_data_path) as f:
            expected_df = pd.read_csv(f)

        # create connector instance
        with db_tools.DBConn(self.dbpath) as conn:
            # The actual test
            conn.append_db_scd_type_two(sparkdf_mock, table_name)
            updated_tbl_query = f"SELECT * FROM {table_name}"
            result_final_df = pd.read_sql_query(updated_tbl_query, conn.conn)
            pd.testing.assert_frame_equal(result_final_df, expected_df)

    def test__scd_column_filters(self):
        with db_tools.DBConn(self.dbpath) as conn:
            sql_filter = conn._scd_column_filters(table_name=self.tbl_name, temp_table_name="temp_loss_item")
            expected_filter = """temp_loss_item.conflict = loss_item.conflict AND temp_loss_item.party = loss_item.party AND temp_loss_item.category_name = loss_item.category_name AND temp_loss_item.type_name = loss_item.type_name AND temp_loss_item.loss_id = loss_item.loss_id AND temp_loss_item.loss_type = loss_item.loss_type AND temp_loss_item.proof_id = loss_item.proof_id"""
            self.maxDiff = None
            self.assertEqual(sql_filter, expected_filter)


class TestIntegrationDBLocal_summary(TestCase):
    """Same test as above, but now on summary tables"""
    def setUp(self):
        # setup general permanent data table
        tempdir = mkdtemp()
        dbname = 'testdb'
        self.dbpath = Path(tempdir) / dbname
        self.conn = sqlite3.connect(self.dbpath)
        self.cursor = self.conn.cursor()
        config_path = Path(__file__).parent.parent.parent / 'config' / "db" / "oryxloss_schema_rolling.yaml"
        with open(config_path) as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)

        self.tbl_name = "summary"

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

        # adding data
        self.base_data_path = Path("../data") / Path("loss_input_5_basic_summary.csv")
        with open(self.base_data_path) as f:
            self.data = pd.read_csv(f)
        self.data.to_sql(self.tbl_name, self.conn, if_exists="replace", index=False)


        # reading in temp table data and saving as formatted version for simpler testing downstream
        temp_data_path = Path("../data") / Path("loss_input_4_summary.csv")
        with open(temp_data_path) as f:
            temp_data = pd.read_csv(f)
        temp_data["start_date"] = temp_data["as_of"]
        temp_data["stop_date"] = temp_data["as_of"]
        temp_data = temp_data.drop("as_of", axis=1)
        temp_data = temp_data[
            ["start_date", "stop_date"] + [col for col in temp_data.columns if col not in ["start_date", "stop_date"]]]
        self.temp_data = temp_data

    def test_setup(self):
        # Query what tables are in database
        # If this fails, setup is not creating the expected table

        # Checking if table exists
        query = f"SELECT * FROM sqlite_master WHERE type='table' and name='{self.tbl_name}'"
        sqlite_master_result = self.cursor.execute(query).fetchall()
        self.assertEqual(len(sqlite_master_result), 1)

        # Checking if table is empty
        query_tbl = f"SELECT * FROM {self.tbl_name}"
        result = self.cursor.execute(query_tbl).fetchall()
        self.assertEqual(len(result), len(self.data))

        # Checking if columns are as expected
        columns = [description[0] for description in self.cursor.description] if self.cursor.description else []
        expected_columns = ['start_date', 'stop_date', 'conflict', 'party', 'category_name', 'destroyed', 'damaged', 'abandoned', 'captured', 'total']
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
            expected_columns = ['start_date', 'stop_date', 'conflict', 'party', 'category_name', 'destroyed', 'damaged', 'abandoned', 'captured', 'total']
            self.assertEqual(columns, expected_columns)
            query = f"PRAGMA table_info(temp_{self.tbl_name})"
            result = conn.cursor.execute(query).fetchall()
            self.assertEqual(len(result), len(expected_columns))

    def test_format_df_for_temp_storage(self):
        data_path = Path("../data") / Path("loss_input_4_summary.csv")
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

    def test__sql_scd_insert_new(self):
        sql_filters = """temp_summary.conflict = summary.conflict AND temp_summary.party = summary.party AND temp_summary.category_name = summary.category_name AND temp_summary.destroyed = summary.destroyed AND temp_summary.damaged = summary.damaged AND temp_summary.abandoned = summary.abandoned AND temp_summary.captured = summary.captured AND temp_summary.total = summary.total"""
        instance = db_tools.DBConn(self.dbpath)
        sql = instance._sql_scd_insert_new(self.tbl_name, f"temp_{self.tbl_name}", sql_filters)
        expected = f"""
        INSERT INTO {self.tbl_name}
        SELECT start_date, "2222-12-31" as stop_date, s1.conflict, s1.party, s1.category_name, s1.destroyed, s1.damaged, s1.abandoned, s1.captured, s1.total
        FROM temp_{self.tbl_name} s1
        WHERE NOT EXISTS (
        SELECT 1
        FROM {self.tbl_name} s2
        WHERE s1.conflict = s2.conflict AND s1.party = s2.party AND s1.category_name = s2.category_name AND s1.destroyed = s2.destroyed AND s1.damaged = s2.damaged AND s1.abandoned = s2.abandoned AND s1.captured = s2.captured AND s1.total = s2.total
        )
        """
        self.maxDiff = None
        self.assertEqual(sql, expected)

    def test__sql_scd_close_outdated(self):
        sql_filters = """temp_summary.conflict = summary.conflict AND temp_summary.party = summary.party AND temp_summary.category_name = summary.category_name AND temp_summary.destroyed = summary.destroyed AND temp_summary.damaged = summary.damaged AND temp_summary.abandoned = summary.abandoned AND temp_summary.captured = summary.captured AND temp_summary.total = summary.total"""
        instance = db_tools.DBConn(self.dbpath)
        sql = instance._sql_scd_close_outdated(self.tbl_name, f"temp_{self.tbl_name}", sql_filters)
        expected = f"""
        UPDATE {self.tbl_name}
        SET stop_date = ?
        WHERE NOT EXISTS (
        SELECT 1
        FROM temp_{self.tbl_name}
        WHERE temp_summary.conflict = summary.conflict AND temp_summary.party = summary.party AND temp_summary.category_name = summary.category_name AND temp_summary.destroyed = summary.destroyed AND temp_summary.damaged = summary.damaged AND temp_summary.abandoned = summary.abandoned AND temp_summary.captured = summary.captured AND temp_summary.total = summary.total
        )
        """
        self.assertEqual(sql, expected)

    def test_execute_scd_update(self):
        table_name = self.tbl_name
        temp_data = self.temp_data

        expected_data_path = Path("../data") / Path("loss_input_6_expected_summary.csv")
        with open(expected_data_path) as f:
            expected_df = pd.read_csv(f)

        temp_table_name = f"temp_{table_name}"
        temp_tbl_sql = f"""CREATE TEMP TABLE "{temp_table_name}" (
                          "start_date" TEXT,
                          "stop_date" TEXT,
                          "conflict" TEXT,
                          "party" TEXT,
                          "category_name" TEXT,
                          "destroyed" INTEGER,
                          "damaged" INTEGER,
                          "abandoned" INTEGER,
                          "captured" INTEGER,
                          "total" INTEGER
                          )"""

        # create connector (temp table exists only withint he same session)
        with db_tools.DBConn(self.dbpath) as conn:
            # create temp table and load data
            conn.cursor.execute(temp_tbl_sql)
            temp_data.to_sql(temp_table_name, conn.conn, if_exists="replace", index=False)

            # test if temp loading was successfully
            query_tbl = f"SELECT * FROM {temp_table_name}"
            result_df = pd.read_sql_query(query_tbl, conn.conn)
            self.assertEqual(len(result_df), len(temp_data))
            pd.testing.assert_frame_equal(result_df, temp_data)

            # The actual test
            conn._execute_scd_update(table_name, temp_table_name, "2025-04-01")
            updated_tbl_query = f"SELECT * FROM {table_name}"
            result_final_df = pd.read_sql_query(updated_tbl_query, conn.conn)
            pd.testing.assert_frame_equal(result_final_df, expected_df)

    # Testing appending data to temp_table
    def test_append_db_scd_type_two(self):
        # Setting up data and mock to create temp table
        sparkdf_mock = MagicMock()
        sparkdf_mock.toPandas.return_value = self.temp_data
        table_name = self.tbl_name

        expected_data_path = Path("../data") / Path("loss_input_6_expected_summary.csv")
        with open(expected_data_path) as f:
            expected_df = pd.read_csv(f)

        # create connector instance
        with db_tools.DBConn(self.dbpath) as conn:
            # The actual test
            conn.append_db_scd_type_two(sparkdf_mock, table_name)
            updated_tbl_query = f"SELECT * FROM {table_name}"
            result_final_df = pd.read_sql_query(updated_tbl_query, conn.conn)
            pd.testing.assert_frame_equal(result_final_df, expected_df)

    def test__scd_column_filters(self):
        with db_tools.DBConn(self.dbpath) as conn:
            sql_filter = conn._scd_column_filters(table_name=self.tbl_name, temp_table_name=f"temp_{self.tbl_name}")
            expected_filter = """temp_summary.conflict = summary.conflict AND temp_summary.party = summary.party AND temp_summary.category_name = summary.category_name AND temp_summary.destroyed = summary.destroyed AND temp_summary.damaged = summary.damaged AND temp_summary.abandoned = summary.abandoned AND temp_summary.captured = summary.captured AND temp_summary.total = summary.total"""
            self.maxDiff = None
            self.assertEqual(sql_filter, expected_filter)


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
