"""

"""
from typing import Union, Optional
from pathlib import Path
import logging

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col, split, explode

from src.sparkutil import ETL, ETLJob, trim_df, create_spark_session, add_metadata
from src.metadata import MetaGenerator
from src.jobs import ETLCLASSES


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
        logger.debug(f"Created ETLExecutor")

    def run_all(self):
        for task in self.config["tasks"]:
            logger.debug(f"\nRunning task: {task}\n")
            metadata = self.get_metadata(task)
            task_class = self.classes[task["class"]]
            self.run_task(etl_class=task_class, logic=task["steps"],
                          out_dir=task["out"], metadata=metadata)

        logger.info("ETL runs completed!")

    # method to run single ETL job
    def run_task(self, etl_class: ETL, logic: str, out_dir, metadata: Optional[dict] = None):
        etl = etl_class(self.source, self.spark, metadata)
        if logic == "etl":
            etl.extract()
            etl.transform()
            etl.load(out_dir)

    def get_metadata(self, config) -> dict:
        if not config.get("metadata"):
            logger.debug("No metadata in config")
            return None
        return self.meta_generator.run(config["metadata"])
