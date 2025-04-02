"""Metadata generator:
Creating additional data from static config or from file to be added downstream
"""
import re
import logging
from typing import Union
from pathlib import Path


logger = logging.getLogger(__name__)


class MetaGenerator:
    def __init__(self, source: Union[Path, str]):
        self.source = source
        self.config = None

    def run(self, config: dict) -> dict:
        logger.debug(f"Running metagenerator for config {config}")
        self.config = config
        value_pairs = {}
        if self.config.get("generated"):
            logger.debug("Running generated rules...")
            for subrule in self.config["generated"]:
                logger.debug(f"Running for subrule:{subrule}")
                value_pairs[subrule["column"]] = self._match_method(subrule)
        if self.config.get("static"):
            logger.debug("Running static rules...")
            for subrule in self.config["static"]:
                value_pairs[subrule["column"]] = value_pairs[subrule["value"]]

        logger.debug(f"Metadata created: {value_pairs}")
        return value_pairs

    def resolve_from_filename(self, k_v_pairs: dict):
        filename = self._parse_filename()
        match = [key for key in k_v_pairs if key in filename]
        logger.debug(f"Picked up string '{match}' from file {filename}")

        if len(match) == 1:
            return k_v_pairs[match[0]]
        elif len(match) > 1:
            raise Exception(f"Multiple resolution strings found in {filename} for {k_v_pairs}!")
        else:
            raise Exception(f"Could not resolve matches in {filename} for {k_v_pairs}!")

    def date_from_filename(self):
        filename = self._parse_filename()
        date_pattern = r"(\d{4}-\d{2}-\d{2})"
        date = re.search(date_pattern, filename).group(1)
        logger.debug(f"Picked up date '{date}' from file {filename}")
        if date:
            return date
        else:
            raise Exception(f"No date in filename {filename}!")

    def _match_method(self, config: dict) -> dict:
        match config:
            case {"method": "date_from_filename"}:
                return self.date_from_filename()

            case {"method": "resolve_from_filename", "args": args}:
                return self.resolve_from_filename(args)

            case _:
                raise Exception("Metadata method not found")

    def _parse_filename(self):
        file_path = self.source
        return Path(file_path).name
