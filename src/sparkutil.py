"""
Spark specific utility and abstractions
"""
from abc import ABC, abstractmethod
from typing import Union
from pathlib import Path

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import trim


# class for holding/governing/getting Spark session
def create_spark_session(appname: str, config: dict) -> SparkSession:
    builder = SparkSession.builder.appName(appname)
    for key, value in configs.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()

# Abstract class for spark jobs to be implemented
class ETL(ABC):
    def __init__(self, source: Union[Path, str], sparks_ession):
        self.source = source
        self.sparks_ession = sparks_ession
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
    def __init__(self, etl_logic: ETL, source: Union[Path, str], output: Union[Path, src], extra_args: dict):
        self.etl = etl_logic
        self.source = source
        self.output = output
        self.args = extra_args

# Input is spark session, data(frame?) and cols to keep/remove


def trim_df(spark_df):
    """
    Removing whitespace from start/end of all columns
    """
    trimmed_df = spark_df.select([trim(sparkdf[col]).alias(col) for col in spark_df.columns])
    return trimmed_df
