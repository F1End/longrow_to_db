"""
Spark specific utility and abstractions
"""
from abc import ABC, abstractmethod
from typing import Union
from pathlib import Path

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import trim
from pyspark.sql import dataframe


config = {"spark.executor.memory": "1g",
          "spark.driver.memory": "1g",
          "spark.executor.cores": "2"}

# class for holding/governing/getting Spark session
def create_spark_session(appname: str, config: dict) -> SparkSession:
    builder = SparkSession.builder.appName(appname)
    for key, value in config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()

# Abstract class for spark jobs to be implemented
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


# ETL job, including input and output
class ETLJob:
    def __init__(self, etl_logic: ETL, source: Union[Path, str], output: Union[Path, str], extra_args: dict):
        self.etl = etl_logic
        self.source = source
        self.output = output
        self.args = extra_args

# Input is spark session, data(frame?) and cols to keep/remove


def trim_df(spark_df: dataframe) -> dataframe:
    """
    Removing whitespace from start/end of all columns
    """
    trimmed_df = spark_df.select([trim(spark_df[col]).alias(col) for col in spark_df.columns])
    return trimmed_df

def add_metadata(spark_df: dataframe, metadata: dict, leftside_insert: bool = False) -> dataframe:
    for key, value in metadata.items():
        spark_df = spark_df.withColumn(key, lit(value))
    if leftside_insert:
        new_col_order = list(metadata.keys()) + [col for col in spark_df.columns if col not in metadata]
        spark_df = spark_df.select(*new_col_order)

    return spark_df

