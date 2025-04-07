"""
Spark specific utility and abstractions
"""
from abc import ABC, abstractmethod
import logging
from typing import Union, Optional
from pathlib import Path

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import trim, lit
from pyspark.sql import dataframe


logger = logging.getLogger(__name__)

config = {"spark.executor.memory": "1g",
          "spark.driver.memory": "1g",
          "spark.executor.cores": "2"}


def create_spark_session(appname: str, config: dict) -> SparkSession:
    logger.info(f"Creating spark session '{appname}' from config {config}")
    builder = SparkSession.builder.appName(appname)
    for key, value in config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


class ETL(ABC):
    def __init__(self, source: Union[Path, str], spark_session: SparkSession):
        self.source = source
        self.spark_session = spark_session
        self.data = None

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass


def persist_data(self, out_path: Optional[Union[Path, str]] = None, db_table: Optional[str] = None):
    out_path = str(out_path)
    if out_path:
        logger.info(f"Saving data to {out_path}")
        self.data.write.option("header", True).mode("overwrite").csv(out_path)

    if db_table and not self.db:
        raise Exception(f"Database table target provided but instance is without database connection!")

    if db_table and self.db:
        with self.db as db_connection:
            logger.debug(f"Pushing data to table {db_table} in {self.db.db_path}")
            db_connection.append_db(self.data, db_table)


def trim_df(spark_df: dataframe) -> dataframe:
    """
    Removing whitespace from start/end of all columns
    """
    trimmed_df = spark_df.select([trim(spark_df[col]).alias(col) for col in spark_df.columns])
    return trimmed_df


def add_metadata(spark_df: dataframe, metadata: dict, leftside_insert: bool = False) -> dataframe:
    logger.debug(f"Adding metadata {metadata} to {spark_df}")
    for key, value in metadata.items():
        spark_df = spark_df.withColumn(key, lit(value))
    if leftside_insert:
        new_col_order = list(metadata.keys()) + [col for col in spark_df.columns if col not in metadata]
        spark_df = spark_df.select(*new_col_order)

    return spark_df
