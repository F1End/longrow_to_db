"""
Generic utility
"""
from pathlib import Path
from typing import Union

import yaml


def parse_args():
    parser = ArgumentParser(description="Longrow csv parser")
    parser.add_argument("--base_config", default="./config/default_config.yaml",
                        help="Yaml file for base Spark settings")
    parser.add_argument("--job_config",
                        help="Yaml file for tasks to be executed")
    parser.add_argument("--data_file",
                        help="CSV file with processable data")
    parser.add_argument("--debug", default=False,
                        action=BooleanOptionalAction,
                        help="Set logger level to debug")

    arguments = parser.parse_args()

    return arguments


def parse_yaml(yaml_path: Union[Path, str]) -> dict:
    with open(yaml_path, "r") as file:
        data = yaml.safe_load(file)

    return data


class metadata_generator:
    def __init__(self):
        patterns = []

    def from_filename(self, file_path):
        raise NotImplemented

    def _parse_filename(self, file_path):
        raise NotImplemented
