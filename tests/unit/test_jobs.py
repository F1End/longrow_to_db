from unittest import TestCase, main
from unittest.mock import patch, MagicMock, call
import sqlite3
from tempfile import mkdtemp
import os
from pathlib import Path
import logging
import subprocess
import sys
import filecmp

from src.jobs import OryxLossesItemSCD2
from src.sparkutil import ETL, trim_df, create_spark_session, add_metadata
from src.util import parse_yaml


root = Path(__file__).parent.parent.parent
config_path = root / "config"
data_path = root / "tests" / "data"

class TestOryxLossesItem(TestCase):
    def test_init(self):
        pass


class TestOryxLossesProofs(TestCase):
    def test_init(self):
        pass


class TestOryxLossesSummary(TestCase):
    def test_init(self):
        pass


class TestOryxLossesCategories(TestCase):

    def test_integration(self):
        pass

class TestOryxLossesItemSCD2(TestCase):

    def test_integration(self):
        # Case 1: Breaking of numbered-merged rows

        # tempdir = Path(mkdtemp())
        # test_db = "wartracker_regression.db"
        # db_path =
        spark_config = parse_yaml(config_path / "default_config.yaml")

        file_in = data_path / "2025-04-25_parsing_test_1-ukrainian_parsed.csv"
        appname = "testETL"
        spark_session = create_spark_session(appname=appname, config=spark_config)

        instance = OryxLossesItemSCD2(source=str(file_in),
                                      spark=spark_session,
                                      db_conn=None,
                                      metadata=None)

        instance.extract()
        instance.transform()

        # instance._trim_df()
        # print(1)
        # pd_df = instance.data.toPandas()
        # print(pd_df.to_string())
        # instance._filter_base_cols()
        # print(2)
        # pd_df = instance.data.toPandas()
        # print(pd_df.to_string())
        # instance._build_cleaned_items()
        # print(3)
        # pd_df = instance.data.toPandas()
        # print(pd_df.to_string())
        # instance._split_to_losses()
        # print(4)
        # pd_df = instance.data.toPandas()
        # print(pd_df.to_string())
        # instance._split_loss_to_rows()
        # print(5)
        # pd_df = instance.data.toPandas()
        # print(pd_df.to_string())
        # instance._remove_surplus_loss_data()
        # print(6)
        # pd_df = instance.data.toPandas()
        # print(pd_df.to_string())
        # instance._filter_final_cols()
        # print(7)
        # pd_df = instance.data.toPandas()
        # print(pd_df.to_string())

        pd_df = instance.data.toPandas()
        print(pd_df.to_string())

        # Case 2: Running a whole file


if __name__ == '__main__':
    main()
