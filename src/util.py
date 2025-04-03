"""
Generic utility
"""
from pathlib import Path
from typing import Union
import re
import logging
from argparse import ArgumentParser, BooleanOptionalAction

import yaml


logger = logging.getLogger(__name__)


def parse_args():
    parser = ArgumentParser(description="Longrow csv parser")
    parser.add_argument("--base_config", default="./config/default_config.yaml",
                        help="Yaml file for base Spark settings")
    parser.add_argument("--job_config",
                        help="Yaml file for tasks to be executed")
    parser.add_argument("--data_file",
                        help="CSV file with processable data")
    parser.add_argument("--db_path",
                        help="Path to database for data storage/lookup")
    parser.add_argument("--init_db",
                        help="Yaml file for db/table config to be created (if does not exists already)")
    parser.add_argument("--debug", default=False,
                        action=BooleanOptionalAction,
                        help="Set logger level to debug")

    arguments = parser.parse_args()

    return arguments


def parse_yaml(yaml_path: Union[Path, str]) -> dict:
    with open(yaml_path, "r") as file:
        data = yaml.safe_load(file)

    return data


def init_logging(debug: bool = False):
    level = logging.INFO if not debug else logging.DEBUG
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=level
    )
    # Setting logger for py4j due to flood during debug
    logging.getLogger("py4j").setLevel(logging.WARNING)
