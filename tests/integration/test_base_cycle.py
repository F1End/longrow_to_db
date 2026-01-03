from unittest import TestCase, main
from unittest.mock import patch, MagicMock, call
import sqlite3
from tempfile import mkdtemp
# import tempfile
import os
from pathlib import Path
import logging
import subprocess
import sys
import filecmp

import pandas as pd
import yaml

from src import db_tools
from src.tst_utils import compare_dataframes

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)


# class TestOryxSchema(TestCase):
#
#     def test_oryxloss_schema(self):
#         root = Path(__file__).parent.parent.parent
#         config_path = root / "config"
#         data_path = root / "tests" / "data"
#
#         # loading expected values
#         expected_master_df = pd.read_csv(data_path / "integration_sqlite_master.csv")
#         expected_summary_df = pd.read_csv(data_path / "integration_summary.csv")
#         expected_loss_df = pd.read_csv(data_path / "integration_loss_item_joined.csv")
#         expected_proof_df = pd.read_csv(data_path / "integration_proofs_ordered.csv")
#
#         input_files = ["2025-04-24_attack-on-europe-documenting-ukrainian_parsed.csv",
#                        "2025-04-25_attack-on-europe-documenting-ukrainian_parsed.csv",
#                        "2025-04-26_attack-on-europe-documenting-ukrainian_parsed.csv",
#                        "2025-04-24_attack-on-europe-documenting-equipment_parsed.csv",
#                        "2025-04-25_attack-on-europe-documenting-equipment_parsed.csv",
#                        "2025-04-26_attack-on-europe-documenting-equipment_parsed.csv"]
#
#         # with tempfile.TemporaryDirectory() as tmpdir:
#         tempdir = Path(mkdtemp())
#         test_db = "wartracker_regression.db"
#
#         base_cmd = [sys.executable,
#                     root / "main.py",
#                     "--base_config", config_path / "default_config.yaml",
#                     "--job_config", config_path / "oryxloss.yaml",
#                     "--init_db", config_path / "db" / "oryxloss_schema.yaml",
#                     "--db_path", tempdir / test_db,
#                     "--data_file"]
#
#         commands = []
#
#         for input_file in input_files:
#             commands.append(base_cmd + [data_path / input_file])
#
#         # run commands
#         for command in commands:
#             print("Running command:", command)
#             result = subprocess.run(command, capture_output=True, text=True, check=True)
#
#         new_db_file = tempdir / test_db
#         conn = sqlite3.connect(new_db_file)
#
#         query_master = """SELECT * FROM sqlite_master"""
#         query_summary = """SELECT * FROM summary"""
#         query_loss_item = """SELECT li.as_of, li.conflict, li.party, li.category_name, li.type_name, li.loss_id, li.loss_type, p.proof
#                              FROM loss_item li INNER JOIN proofs p ON li.proof_id = p.id"""
#         query_proofs = """SELECT proof FROM proofs ORDER BY proof"""
#
#         master_df = pd.read_sql_query(query_master, conn)
#         summary_df = pd.read_sql_query(query_summary, conn)
#         loss_df = pd.read_sql_query(query_loss_item, conn)
#         proof_df = pd.read_sql_query(query_proofs, conn)
#         loss_df["proof"] = loss_df["proof"].str.strip()  # it seems sometimes trailing spaces are added by pandas query?
#
#         pd.testing.assert_frame_equal(master_df, expected_master_df)
#         pd.testing.assert_frame_equal(summary_df, expected_summary_df)
#         pd.testing.assert_frame_equal(loss_df, expected_loss_df)
#         pd.testing.assert_frame_equal(proof_df, expected_proof_df)

class TestOryxSchemaSCD2(TestCase):

    def test_oryxloss_schema_rolling(self):
        root = Path(__file__).parent.parent.parent
        config_path = root / "config"
        data_path = root / "tests" / "data"


        # loading expected values
        expected_master_df = pd.read_csv(data_path / "integration_sqlite_master_scd2.csv")
        expected_summary_df = pd.read_csv(data_path / "integration_summary_scd3.csv")
        expected_loss_df = pd.read_csv(data_path / "integration_loss_scd2_expected.csv")
        expected_loss_0424_df = pd.read_csv(data_path / "integration_loss_scd2_expected_0424.csv")
        expected_loss_0425_df = pd.read_csv(data_path / "integration_loss_scd2_expected_0425.csv")
        expected_loss_0426_df = pd.read_csv(data_path / "integration_loss_scd2_expected_0426.csv")
        expected_proof_df = pd.read_csv(data_path / "integration_proofs_ordered.csv")
        expected_category_df = pd.read_csv(data_path / "integration_category.csv")[["category"]]

        input_files = ["2025-04-24_attack-on-europe-documenting-ukrainian_parsed.csv",
                       "2025-04-25_attack-on-europe-documenting-ukrainian_parsed.csv",
                       "2025-04-26_attack-on-europe-documenting-ukrainian_parsed.csv",
                       "2025-04-24_attack-on-europe-documenting-equipment_parsed.csv",
                       "2025-04-25_attack-on-europe-documenting-equipment_parsed.csv",
                       "2025-04-26_attack-on-europe-documenting-equipment_parsed.csv"
                       ]

        input_files = ["2025-04-24_attack-on-europe-documenting-equipment_parsed.csv",
                       "2025-04-25_attack-on-europe-documenting-equipment_parsed.csv",
                       "2025-04-26_attack-on-europe-documenting-equipment_parsed.csv",
                       "2025-04-24_attack-on-europe-documenting-ukrainian_parsed.csv",
                       "2025-04-25_attack-on-europe-documenting-ukrainian_parsed.csv",
                       "2025-04-26_attack-on-europe-documenting-ukrainian_parsed.csv"
                       ]
        # input_files = ["2025-04-25_parsing_test_1__-attack-on-europe-documenting-ukrainian-__parsed.csv"]

        # saved_df = pd.read_csv("summary_scd_2_d1.csv")
        #
        # expected_summary_df = expected_summary_df.sort_values(by=["total", "category_name", "stop_date", "start_date"]).reset_index(
        #     drop=True)
        # saved_df = saved_df.sort_values(by=["total", "category_name", "stop_date", "start_date"]).reset_index(
        #     drop=True)
        #
        # expected_summary_df.to_csv("expected_testing_summ_2.csv")
        # saved_df.to_csv("built_testing_summ_2.csv")
        #
        # pd.testing.assert_frame_equal(expected_summary_df, saved_df)

        # with tempfile.TemporaryDirectory() as tmpdir:
        tempdir = Path(mkdtemp())
        test_db = "wartracker_regression.db"

        base_cmd = [sys.executable,
                    root / "main.py",
                    "--base_config", config_path / "default_config.yaml",
                    "--job_config", config_path / "oryxloss_scd2.yaml",
                    "--init_db", config_path / "db" / "oryxloss_schema_rolling.yaml",
                    "--db_path", tempdir / test_db,
                    "--data_file"]

        commands = []

        for input_file in input_files:
            commands.append(base_cmd + [data_path / input_file])

        # run commands
        for command in commands:
            try:
                print("Running command:", command)
                result = subprocess.run(command, capture_output=True, text=True, check=True)
                # print("____STDOUT____")
                # print(result.stdout.strip())
                # print("____STDERR____")
                # print(result.stderr.strip())
                # print("______________")
                # new_db_file = tempdir / test_db
                # conn = sqlite3.connect(new_db_file)
                # query_summary = """SELECT * FROM summary"""
                # summ = pd.read_sql_query(query_summary, conn)
                # # print(summ.to_string())
                # conn.close()
            except subprocess.CalledProcessError as e:
                print("Command failed with exit code:", e.returncode)
                print("--- STDOUT ---")
                print(e.stdout)
                print("--- STDERR (likely traceback) ---")
                print(e.stderr)
                raise

        new_db_file = tempdir / test_db
        conn = sqlite3.connect(new_db_file)

        query_master = """SELECT * FROM sqlite_master"""
        query_summary = """SELECT * FROM summary"""
        # query_loss_item = """SELECT li.start_date, li.stop_date, li.conflict, li.party, li.category_name, li.type_name, li.loss_id, li.loss_type, p.proof
        #                      FROM loss_item li INNER JOIN proofs p ON li.proof_id = p.id"""
        query_loss_item = """SELECT li.start_date, li.stop_date, li.conflict, li.party, cn.category, li.type_name, li.loss_id, li.loss_type, p.proof
                             FROM loss_item li INNER JOIN proofs p ON li.proof_id = p.id
                             INNER JOIN category_names cn ON li.category_id = cn.id"""
        # query_loss_item = """SELECT *
        #                      FROM loss_item li INNER JOIN proofs p ON li.proof_id = p.id
        #                      INNER JOIN category_names cn ON li.category_id = cn.id"""
        # query_loss_item_2 = """SELECT * FROM loss_item"""
        query_loss_item_2 = """SELECT *
                                     FROM loss_item li INNER JOIN proofs p ON li.proof_id = p.id
                                     INNER JOIN category_names cn ON li.category_id = cn.id"""
        query_proofs = """SELECT proof FROM proofs ORDER BY proof"""
        query_categories = """SELECT * FROM category_names ORDER BY category"""

        master_df = pd.read_sql_query(query_master, conn)
        summary_df = pd.read_sql_query(query_summary, conn)
        loss_df = pd.read_sql_query(query_loss_item, conn)
        loss_df_2 = pd.read_sql_query(query_loss_item_2, conn)
        proof_df = pd.read_sql_query(query_proofs, conn)
        category_df = pd.read_sql_query(query_categories, conn)
        loss_df["proof"] = loss_df["proof"].str.strip()  # it seems sometimes trailing spaces are added by pandas query?

        # master_df.to_csv("scd2_master_1c_4.csv", index=False)
        # summary_df.to_csv("scd2_summary_1c_5.csv", index=False)
        # loss_df.to_csv("scd2_loss_item_expectedc_6"
        #                ".csv", index=False)
        # loss_df_2.to_csv("scd2_loss_item_20251229-1.csv", index=False)
        # proof_df.to_csv("scd2_proof_1c_4.csv", index=False)
        # category_df.to_csv("scd2_category_1c_4.csv", index=False)
        # loss_df.to_csv("test_loss_df_for_edit2.csv")
        print(loss_df.to_string())

        loss_0424_df = loss_df.loc[loss_df["start_date"].isin(["2025-04-24"])]
        print(f"1: {loss_0424_df.shape}")

        loss_0425_df = loss_df.loc[loss_df["start_date"].isin(["2025-04-24", "2025-04-25"])]
        # loss_0425_df = loss_0425_df.loc[loss_0425_df["stop_date"] == "2222-12-31"]
        loss_0425_df = loss_0425_df.loc[loss_0425_df["stop_date"].isin(["2222-12-31", "2025-04-26"])]
        print(f"2: {loss_0425_df.shape}")

        loss_0426_df = loss_df.loc[loss_df["stop_date"] == "2222-12-31"]
        print(f"3: {loss_0426_df.shape}")

        # loss_0426_df.to_csv("test_loss_0426_c_5.csv", index=False)
        category_df = category_df[["category"]]

        # sort rows for easier comparison
        summary_df = summary_df.sort_values(by=["total", "category_name", "stop_date", "start_date"]).reset_index(drop=True)
        expected_summary_df = expected_summary_df.sort_values(by=["total", "category_name", "stop_date", "start_date"]).reset_index(drop=True)
        loss_0426_df = loss_0426_df.sort_values(by=loss_0426_df.columns.tolist()).reset_index(drop=True)
        expected_loss_0426_df = expected_loss_0426_df.sort_values(by=expected_loss_0426_df.columns.tolist()).reset_index(drop=True)
        loss_df = loss_df.sort_values(by=loss_df.columns.tolist()).reset_index(drop=True)
        expected_loss_df = expected_loss_df.sort_values(by=expected_loss_df.columns.tolist()).reset_index(drop=True)
        summary_df.to_csv("summary_scd_2_e.csv", index=False)
        # expected_summary_df.to_csv("expected_summary_scd_2_d1.csv", index=False)
        # pd.testing.assert_frame_equal(proof_df, expected_proof_df)
        # pd.testing.assert_frame_equal(category_df, expected_category_df)
        # pd.testing.assert_frame_equal(master_df, expected_master_df)

        # Iterating through dates as ordering might mess up assert_frame_equal otherwise
        start_dates = ["2025-04-24", "2025-04-25", "2025-04-26"]
        for date in start_dates:
            filtered_summary_df = summary_df[summary_df["start_date"] == date].reset_index(drop=True)
            filtered_expected_summary_df = expected_summary_df[expected_summary_df["start_date"] == date].reset_index(drop=True)
            pd.testing.assert_frame_equal(filtered_summary_df, filtered_expected_summary_df)

        ########################################
        # EDITING dataframes to test periodic value match

        loss_0424_df = loss_0424_df[[col for col in loss_0424_df.columns if col not in ["stop_date", "start_date"]]]
        loss_0425_df = loss_0425_df[[col for col in loss_0425_df.columns if col not in ["stop_date", "start_date"]]]
        loss_0426_df = loss_0426_df[[col for col in loss_0426_df.columns if col not in ["stop_date", "start_date"]]]
        print(f"4: {loss_0424_df.shape}")
        print(f"5: {loss_0425_df.shape}")
        print(f"6: {loss_0426_df.shape}")

        expected_loss_0424_df = expected_loss_0424_df[
            [col for col in expected_loss_0424_df.columns if col not in ["stop_date", "start_date", "as_of"]]]
        expected_loss_0425_df = expected_loss_0425_df[
            [col for col in expected_loss_0425_df.columns if col not in ["stop_date", "start_date", "as_of"]]]
        expected_loss_0426_df = expected_loss_0426_df[
            [col for col in expected_loss_0426_df.columns if col not in ["stop_date", "start_date", "as_of"]]]

        loss_0424_df = loss_0424_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
            drop=True)
        expected_loss_0424_df = expected_loss_0424_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
            drop=True)

        loss_0425_df = loss_0425_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
            drop=True)
        expected_loss_0425_df = expected_loss_0425_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
            drop=True)

        loss_0426_df = loss_0426_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
            drop=True)
        expected_loss_0426_df = expected_loss_0426_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
            drop=True)
        loss_0425_df.to_csv(data_path / "loss_0425_251223-3b.csv", index=False)
        loss_0424_df.to_csv(data_path / "loss_0424_251223-3b.csv", index=False)
        # loss_0426_df.to_csv(data_path / "loss_0426_251223-3.csv", index=False)

        print(f"7: {loss_0424_df.shape}")
        print(f"8: {loss_0425_df.shape}")
        print(f"9: {loss_0426_df.shape}")

        pd.testing.assert_frame_equal(proof_df, expected_proof_df)
        pd.testing.assert_frame_equal(category_df, expected_category_df)
        pd.testing.assert_frame_equal(master_df, expected_master_df)

        col_list = ["party","category","type_name","loss_type","proof"]

        diff_loss_0424 = compare_dataframes(loss_0424_df, expected_loss_0424_df, compare_cols=col_list)
        print(f"Diff for 0424: {diff_loss_0424.shape}")
        print(diff_loss_0424.to_string())
        diff_loss_0424.to_csv(f"diff_0424.csv", index=False)
        print(f"Crosscheck on all:")
        diff_loss_all_vs_24 = compare_dataframes(loss_0424_df, loss_df_2, compare_cols=col_list)
        print(diff_loss_all_vs_24.to_string())
        # pd.testing.assert_frame_equal(loss_0424_df, expected_loss_0424_df)

        diff_loss_0425 = compare_dataframes(loss_0425_df, expected_loss_0425_df, compare_cols=col_list)
        print(f"Diff for 0425: {diff_loss_0425.shape}")
        print(diff_loss_0425.to_string())
        print(f"Crosscheck on all:")
        diff_loss_all_vs_25 = compare_dataframes(loss_0425_df, loss_df_2, compare_cols=col_list)
        print(diff_loss_all_vs_25.to_string())
        # pd.testing.assert_frame_equal(loss_0425_df, expected_loss_0425_df)

        diff_loss_0426 = compare_dataframes(loss_0426_df, expected_loss_0426_df, compare_cols=col_list)
        print(f"Diff for 0426: {diff_loss_0426.shape}")
        print(diff_loss_0426.to_string())
        pd.testing.assert_frame_equal(loss_0426_df, expected_loss_0426_df)
        # pd.testing.assert_frame_equal(loss_df, expected_loss_df)


        # root = Path(__file__).parent.parent.parent
        # integration = root / "tests" / "integration"
        #
        # loss_df = pd.read_csv(integration / "test_loss_df_for_edit.csv", index_col=0)
        # print(loss_df.shape)
        #
        # loss_0424_df = loss_df.loc[loss_df["start_date"].isin(["2025-04-24"])]
        # print("Initial:")
        #
        # loss_0425_df = loss_df.loc[loss_df["start_date"].isin(["2025-04-24", "2025-04-25"])]
        # loss_0425_df = loss_0425_df.loc[loss_0425_df["stop_date"].isin(["2222-12-31"])]
        # # loss_0425_df = loss_0425_df.loc[loss_0425_df["stop_date"] == ""]
        # print(loss_0425_df.shape)
        #
        # loss_0426_df = loss_df.loc[loss_df["stop_date"] == "2222-12-31"]
        #
        # expected_loss_0424_df = pd.read_csv(data_path / "integration_loss_scd2_expected_0424.csv")
        # expected_loss_0425_df = pd.read_csv(data_path / "integration_loss_scd2_expected_0425.csv")
        # expected_loss_0426_df = pd.read_csv(data_path / "integration_loss_scd2_expected_0426.csv")
        #
        #
        #
        # ########################################
        # # EDITING dataframes to test periodic value match
        #
        # loss_0424_df = loss_0424_df[[col for col in loss_0424_df.columns if col not in ["stop_date", "start_date"]]]
        # loss_0425_df = loss_0425_df[[col for col in loss_0425_df.columns if col not in ["stop_date", "start_date"]]]
        # loss_0426_df = loss_0426_df[[col for col in loss_0426_df.columns if col not in ["stop_date", "start_date"]]]
        #
        # print("Col edited:")
        # print(loss_0425_df.shape)
        #
        # expected_loss_0424_df = expected_loss_0424_df[
        #     [col for col in expected_loss_0424_df.columns if col not in ["stop_date", "start_date", "as_of"]]]
        # expected_loss_0425_df = expected_loss_0425_df[
        #     [col for col in expected_loss_0425_df.columns if col not in ["stop_date", "start_date", "as_of"]]]
        # expected_loss_0426_df = expected_loss_0426_df[
        #     [col for col in expected_loss_0426_df.columns if col not in ["stop_date", "start_date", "as_of"]]]
        #
        # print("Sorted:")
        # print(loss_0425_df.shape)
        #
        # loss_0424_df = loss_0424_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
        #     drop=True)
        # expected_loss_0424_df = expected_loss_0424_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
        #     drop=True)
        #
        # loss_0425_df = loss_0425_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
        #     drop=True)
        # expected_loss_0425_df = expected_loss_0425_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
        #     drop=True)
        #
        # loss_0426_df = loss_0426_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
        #     drop=True)
        # expected_loss_0426_df = expected_loss_0426_df.sort_values(by=["proof", "category", "loss_id"]).reset_index(
        #     drop=True)
        #
        # # print(loss_0424_df.columns)
        # # print(loss_0424_df.head().to_string())
        # # print(expected_loss_0424_df.columns)
        #
        # # loss_0425_df.to_csv("loss_0425_df_review.csv")
        #
        # pd.testing.assert_frame_equal(loss_0424_df, expected_loss_0424_df)
        # pd.testing.assert_frame_equal(loss_0425_df, expected_loss_0425_df)
        # pd.testing.assert_frame_equal(loss_0426_df, expected_loss_0426_df)


if __name__ == '__main__':
    main()