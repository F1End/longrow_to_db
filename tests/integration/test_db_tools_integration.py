from tempfile import mkdtemp
import os
from pathlib import Path
import logging
from unittest.mock import MagicMock, patch
from unittest import TestCase, main
import sqlite3

import pandas as pd
import yaml

from src import db_tools


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class TestIntegrationDBLocal2(TestCase):
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

    @patch("src.db_tools.DBConn._format_df_for_temp_storage")
    def test_append_db_scd_type_two(self, mock_format_df_tmp):
        # data_files = ["raw_temp_df_ukr_loss_item_0424.csv",
        #               "raw_temp_df_ukr_loss_item_0425.csv",
        #               "raw_temp_df_ukr_loss_item_0426.csv"]
        data_files = ["raw_temp_df_ukr_summary_0424.csv",
                      "raw_temp_df_ukr_summary_0425.csv",
                      "raw_temp_df_ukr_summary_0426.csv"]
        data_files_path = Path("../data")
        expected_summ_df = pd.read_csv(data_files_path / "integration_summary_scd2.csv")
        expected_summ_df = expected_summ_df[expected_summ_df["party"] == "Ukraine"]
        file_list = [data_files_path / Path(f) for f in data_files]
        df_list = []
        for file in file_list:
            df = pd.read_csv(file)
            df_list.append(df)

        mock_format_df_tmp.side_effect = df_list
        table_name = "summary"
        fake_sparkdf = MagicMock()
        filter_columns = ["party"]

        for df in df_list:
            with db_tools.DBConn(db_path=self.dbpath) as conn:
                conn.append_db_scd_type_two(pyspark_df=fake_sparkdf,
                                            table_name=table_name,
                                            filter_columns=filter_columns)

        query = "SELECT * FROM summary"
        loss_df = pd.read_sql_query(query, self.conn)
        loss_df.to_csv("db_int_summ_1.csv")
        print(loss_df.to_string())
        pd.testing.assert_frame_equal(loss_df, expected_summ_df)


if __name__ == '__main__':
    main()