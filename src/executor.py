"""

"""
from typing import Union, Optional
from pathlib import Path
import logging

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col, split, explode

from src.sparkutil import ETL, trim_df, create_spark_session, add_metadata
from src.metadata import MetaGenerator
from src.jobs import ETLCLASSES
from src.db_tools import DBConn


logger = logging.getLogger(__name__)


class ETLExecutor:
    def __init__(self, source: Union[Path, str], config: dict, spark_config: dict):
        appname = "BaseETL"
        classes = ETLCLASSES
        self.source = source
        self.config = config
        self.classes = classes
        self.spark = create_spark_session(appname=appname, config=spark_config)
        self.meta_generator = MetaGenerator(self.source)
        self.db = DBConn(config["db"]) if config.get("db") else None
        logger.debug(f"Created ETLExecutor")

    def run_all(self):
        for task in self.config["tasks"]:
            logger.debug(f"\nRunning task: {task}\n")
            metadata = self.get_metadata(task)
            out_dir = self.out_dir_path(self.config["out_dir"], metadata, task["table"])\
                      if self.config.get("out_dir")\
                      else None
            task_class = self.classes[task["class"]]
            self.run_task(etl_class=task_class, logic=task["steps"],
                          out_dir=out_dir, metadata=metadata, table_name=task["table"])

        logger.info("ETL runs completed!")

    def run_task(self, etl_class: ETL, logic: str,
                 out_dir: Optional[Union[Path, str]] = None,
                 metadata: Optional[dict] = None,
                 table_name: Optional[str] = None):
        etl = etl_class(self.source, self.spark, metadata=metadata, db_conn=self.db)
        if logic == "etl":
            etl.extract()
            etl.transform()
            etl.load(path=out_dir, table=table_name)

    def get_metadata(self, config) -> dict:
        if not config.get("metadata"):
            logger.debug("No metadata in config")
            return None
        return self.meta_generator.run(config["metadata"])

    def out_dir_path(self, out_dir: Union[Path, str], metadata: dict, table: str) -> Path:
        data_dir = "_".join([str(tag) for tag in metadata.values()]) + "_" + table
        full_path = Path(out_dir).joinpath(data_dir)
        return Path(full_path)
