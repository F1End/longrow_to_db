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


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestOryxSchema(TestCase):

    def test_oryxloss_schema(self):
        root = Path(__file__).parent.parent.parent
        config_path = root / "config"
        data_path = root / "tests" / "data"

        # loading expected values
        expected_master_df = pd.read_csv(data_path / "integration_sqlite_master.csv")
        expected_summary_df = pd.read_csv(data_path / "integration_summary.csv")
        expected_loss_df = pd.read_csv(data_path / "integration_loss_item_joined.csv")
        expected_proof_df = pd.read_csv(data_path / "integration_proofs_ordered.csv")

        input_files = ["2025-04-24_attack-on-europe-documenting-ukrainian_parsed.csv",
                       "2025-04-25_attack-on-europe-documenting-ukrainian_parsed.csv",
                       "2025-04-26_attack-on-europe-documenting-ukrainian_parsed.csv",
                       "2025-04-24_attack-on-europe-documenting-equipment_parsed.csv",
                       "2025-04-25_attack-on-europe-documenting-equipment_parsed.csv",
                       "2025-04-26_attack-on-europe-documenting-equipment_parsed.csv"]

        # with tempfile.TemporaryDirectory() as tmpdir:
        tempdir = Path(mkdtemp())
        test_db = "wartracker_regression.db"

        base_cmd = [sys.executable,
                    root / "main.py",
                    "--base_config", config_path / "default_config.yaml",
                    "--job_config", config_path / "oryxloss.yaml",
                    "--init_db", config_path / "db" / "oryxloss_schema.yaml",
                    "--db_path", tempdir / test_db,
                    "--data_file"]

        commands = []

        for input_file in input_files:
            commands.append(base_cmd + [data_path / input_file])

        # run commands
        for command in commands:
            print("Running command:", command)
            result = subprocess.run(command, capture_output=True, text=True, check=True)

        new_db_file = tempdir / test_db
        conn = sqlite3.connect(new_db_file)

        query_master = """SELECT * FROM sqlite_master"""
        query_summary = """SELECT * FROM summary"""
        query_loss_item = """SELECT li.as_of, li.conflict, li.party, li.category_name, li.type_name, li.loss_id, li.loss_type, p.proof
                             FROM loss_item li INNER JOIN proofs p ON li.proof_id = p.id"""
        query_proofs = """SELECT proof FROM proofs ORDER BY proof"""

        master_df = pd.read_sql_query(query_master, conn)
        summary_df = pd.read_sql_query(query_summary, conn)
        loss_df = pd.read_sql_query(query_loss_item, conn)
        proof_df = pd.read_sql_query(query_proofs, conn)
        loss_df["proof"] = loss_df["proof"].str.strip()  # it seems sometimes trailing spaces are added by pandas query?

        pd.testing.assert_frame_equal(master_df, expected_master_df)
        pd.testing.assert_frame_equal(summary_df, expected_summary_df)
        pd.testing.assert_frame_equal(loss_df, expected_loss_df)
        pd.testing.assert_frame_equal(proof_df, expected_proof_df)



if __name__ == '__main__':
    main()