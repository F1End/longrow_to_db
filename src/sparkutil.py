"""
Spark specific utility and abstractions
"""
from abc import ABC, abstractmethod
from pyspark.sql.session import SparkSession

# class for holding/governing/getting Spark session
def create_spark_session(appname: str, config: dict) -> SparkSession:
    builder = SparkSession.builder.appName(appname)
    for key, value in configs.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()

# Abstract class for spark jobs to be implemented
class ETL(ABC):
    def __init__(self, source):
        self.source = None
        self.data = None

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass

# Input is spark session, data(frame?) and cols to keep/remove


# Class for column addition/removal request