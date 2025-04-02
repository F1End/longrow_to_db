"""
Actual job definition, preferably based on abstract class from sparkutil
"""
from typing import Union, Optional
from pathlib import Path
import logging

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col, split, explode

from src.sparkutil import ETL, ETLJob, trim_df, create_spark_session, add_metadata
from src.metadata import MetaGenerator

logger = logging.getLogger(__name__)


class OryxLossesItem(ETL):
    def __init__(self, source: Union, spark: SparkSession, metadata: Optional[dict] = None):
        super().__init__(source, spark)
        self.metadata = metadata

    def extract(self):
        logger.info(f"Extracting data from {self.source}")
        self.data = self.spark_session.read.option("header", "true").option("inferSchema", "true").csv(self.source)

    def transform(self):
        logger.debug("Running transformations...")
        self._trim_df()
        self._filter_base_cols()
        self._build_cleaned_items()
        self._split_to_losses()
        self._split_loss_to_rows()
        self._filter_final_cols()
        if self.metadata:
            self.data = add_metadata(self.data, self.metadata, leftside_insert=True)

    def load(self, path: Union[Path, str]):
        logger.info(f"Saving data to {path}")
        self.data.write.option("header", True).mode("overwrite").csv(path)

    def _trim_df(self):
        self.data = trim_df(self.data)

    def _filter_base_cols(self):
        self.data = self.data.select("category_name", "type_name", "type_ttl_count", "loss_item", "loss_proof")

    def _build_cleaned_items(self):
        """
        Moving descriptions with multiple losses into more processable, comma delimited text
        """
        self.data = (self.data.withColumn("loss_item", regexp_replace(col("loss_item"), r"[()]", ""))
                     .withColumn("cleaned_items", regexp_replace(col("loss_item"), "and", ","))
                     .withColumn("cleaned_items", regexp_replace(col("cleaned_items"), r"\b(and)\b", "")))

    def _split_to_losses(self):
        """

        """
        self.data = (self.data.withColumn("loss_id", split(self.data["cleaned_items"], ",\s*"))
                     .withColumn("loss_type", regexp_replace(self.data["loss_item"], r".*?,\s*", "")))

    def _split_loss_to_rows(self):
        """
        Separating ids into multiple rows and removing
        leftover invalid rows (removing those without integer id)
        """
        self.data = (self.data.withColumn("loss_id", explode(self.data["loss_id"]))
                     .withColumn("loss_id", col("loss_id").cast("int"))
                     .filter(col("loss_id").isNotNull()))

    def _filter_final_cols(self):
        self.data = self.data.select("category_name", "type_name", "loss_id", "loss_type", "loss_proof")


class OryxLossesProofs(ETL):
    def __init__(self, source: Union[Path, str], spark: SparkSession, metadata: Optional = None):
        super().__init__(source, spark)
        self.metadata = metadata

    def extract(self):
        logger.info(f"Extracting data from {self.source}")
        self.data = self.spark_session.read.option("header", "true").option("inferSchema", "true").csv(self.source)

    def transform(self):
        logger.debug("Running transformations...")
        self._trim_df()
        self.data = self.data.select("loss_proof").distinct()
        if self.metadata:
            self.data = add_metadata(self.data, self.metadata, leftside_insert=True)

    def load(self, path: Union[Path, str]):
        logger.info(f"Saving data to {path}")
        self.data.write.option("header", True).mode("overwrite").csv(path)

    def _trim_df(self):
        self.data = trim_df(self.data)


class OryxLossesSummary(ETL):
    def __init__(self, source: Union[Path, str], spark: SparkSession, metadata: Optional = None):
        super().__init__(source, spark)
        self.metadata = metadata

    def extract(self):
        logger.info(f"Extracting data from {self.source}")
        self.data = self.spark_session.read.option("header", "true").option("inferSchema", "true").csv(self.source)

    def transform(self):
        logger.debug("Running transformations...")
        self._filter_base_cols()
        self._extract_data()
        self._filter_final_cols()
        self._fill_na_with_null()
        if self.metadata:
            self.data = add_metadata(self.data, self.metadata, leftside_insert=True)

    def load(self, path: Union[Path, str]):
        logger.info(f"Saving data to {path}")
        self.data.write.option("header", True).mode("overwrite").csv(path)

    def _filter_base_cols(self):
        self.data = self.data.select("category_counter", "category_name", "category_summary").distinct()

    def _extract_data(self):
        self.data = (self.data
                     .withColumn("destroyed", regexp_extract(self.data["category_summary"], r"destroyed:\s*(\d+)", 1)
                                 .cast("int"))
                     .withColumn("damaged", regexp_extract(self.data["category_summary"], r"damaged:\s*(\d+)", 1)
                                 .cast("int"))
                     .withColumn("abandoned", regexp_extract(self.data["category_summary"], r"abandoned:\s*(\d+)", 1)
                                 .cast("int"))
                     .withColumn("captured", regexp_extract(self.data["category_summary"], r"captured:\s*(\d+)", 1)
                                 .cast("int"))
                     .withColumn("total", regexp_extract(self.data["category_summary"], r"(\d+),", 1)
                                 .cast("int"))
                     )

    def _fill_na_with_null(self):
        self.data = self.data.fillna(0, subset=["destroyed", "damaged",
                                                "abandoned", "captured"])

    def _filter_final_cols(self):
        self.data = self.data.select("category_counter", "category_name", "destroyed",
                                     "damaged", "abandoned", "captured", "total")


ETLCLASSES = {"OryxLossesSummary": OryxLossesSummary,
              "OryxLossesItem": OryxLossesItem,
              "OryxLossesProofs": OryxLossesProofs}
