"""
Actual job definition, preferably based on abstract class from sparkutil
"""
from typing import Union, Optional
from pathlib import Path
import logging
from itertools import chain

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col, split, explode, udf, create_map, lit, trim
from pyspark.sql.types import IntegerType
from pyspark.sql import dataframe

from src.sparkutil import ETL, trim_df, create_spark_session, add_metadata, persist_data, persist_data_scd2
from src.metadata import MetaGenerator
from src.db_tools import DBConn


logger = logging.getLogger(__name__)


class OryxLossesItem(ETL):
    def __init__(self, source: Union[Path, str], spark: SparkSession,
                 metadata: Optional = None, db_conn: Optional = None):
        super().__init__(source, spark)
        self.metadata = metadata
        self.db = db_conn
        self.proof_keys = {}

    def extract(self):
        logger.info(f"Extracting data from {self.source}")
        self.data = self.spark_session.read.option("header", "true").option("inferSchema", "true").csv(self.source)
        if self.db:
            self.proof_keys = self._get_proof_keys()

    def transform(self):
        logger.debug("Running transformations...")
        self._trim_df()
        self._filter_base_cols()
        self._build_cleaned_items()
        self._split_to_losses()
        self._split_loss_to_rows()
        self._remove_surplus_loss_data()
        self._filter_final_cols()
        if self.metadata:
            self.data = add_metadata(self.data, self.metadata, leftside_insert=True)
        if self.proof_keys:
            self._replace_url_with_keys()

    def load(self, path: Optional[Union[Path, str]] = None, table: Optional[str] = None):
        persist_data(self, out_path=path, db_table=table)

    def _trim_df(self):
        self.data = trim_df(self.data)

    def _filter_base_cols(self):
        self.data = self.data.select("category_name", "type_name", "type_ttl_count", "loss_item", "loss_proof")

    def _build_cleaned_items(self):
        """
        Moving descriptions with multiple losses into more processable, comma delimited text
        """
        self.data = (
            self.data
            # 1. Remove parentheses
            .withColumn(
                "cleaned_items",
                regexp_replace(col("loss_item"), r"[()]", "")
            )

            # 2. Replace "and" ONLY when it connects numbers → comma
            .withColumn(
                "cleaned_items",
                regexp_replace(
                    col("cleaned_items"),
                    r"(?<=\d)\s*,?\s*and\s+(?=\d)",
                    ","
                )
            )

            # 3. Add missing commas between numbers
            #    "69 70" → "69,70"
            .withColumn(
                "cleaned_items",
                regexp_replace(
                    col("cleaned_items"),
                    r"(?<=\d)\s+(?=\d)",
                    ","
                )
            )

            # 4. Normalize comma spacing
            .withColumn(
                "cleaned_items",
                regexp_replace(
                    col("cleaned_items"),
                    r"\s*,\s*",
                    ","
                )
            )

            # 5. Insert comma between numeric block and following text
            #    "133 destroyed" → "133,destroyed"
            .withColumn(
                "cleaned_items",
                regexp_replace(
                    col("cleaned_items"),
                    r"(?<=\d)\s+(?=[A-Za-z])",
                    ","
                )
            )

            # 6. Insert comma after quoted ship name if followed by text
            #    "'Donbass' sunk" → "'Donbass', sunk"
            .withColumn(
                "cleaned_items",
                regexp_replace(
                    col("cleaned_items"),
                    r"'\s+(?=[A-Za-z])",
                    "', "
                )
            )

            # 7. Trim
            .withColumn(
                "cleaned_items",
                regexp_replace(col("cleaned_items"), r"^\s+|\s+$", "")
            )
        )

    def _split_to_losses(self):
        """

        """
        self.data = (self.data
                     .withColumn("loss_id", split(self.data["cleaned_items"], ",\s*"))
                     .withColumn("loss_type", regexp_replace(self.data["cleaned_items"], r".*?,\s*", "")))

    def _split_loss_to_rows(self):
        """
        Separating ids into multiple rows and removing
        leftover invalid rows (removing those without integer id)
        """
        self.data = (self.data.withColumn("loss_id", explode(self.data["loss_id"]))
                     .withColumn("loss_id", col("loss_id").cast("int"))
                     .filter(col("loss_id").isNotNull()))

    def _remove_surplus_loss_data(self):
        # removing ship names and other leftover references left due to variance in input formatting
        self.data = self.data.withColumn("loss_type", regexp_replace(col("loss_type"), r"'\s*[^']*?\s*'", ""))

    def _filter_final_cols(self):
        self.data = self.data.select("category_name", "type_name", "loss_id", "loss_type", "loss_proof")

    def _get_proof_keys(self) -> dict:
        with self.db as db_connection:
            proofs_and_keys = db_connection.fetch_unique_data(self.data, "loss_proof", "proofs", "proof")
        logger.debug(f"Fetched {len(proofs_and_keys)} proofs for look-up.")
        proofs_and_keys = {proof: key for key, proof in proofs_and_keys}
        return proofs_and_keys

    def _replace_url_with_keys(self):
        mapping = create_map([lit(x) for x in chain(*self.proof_keys.items())])
        self.data = self.data.withColumn("proof_id", mapping[self.data["loss_proof"]])
        self.data = self.data.drop("loss_proof")


class OryxLossesProofs(ETL):
    def __init__(self, source: Union[Path, str], spark: SparkSession,
                 metadata: Optional = None, db_conn: Optional = None):
        super().__init__(source, spark)
        self.metadata = metadata
        self.db = db_conn

    def extract(self):
        logger.info(f"Extracting data from {self.source}")
        self.data = self.spark_session.read.option("header", "true").option("inferSchema", "true").csv(self.source)

    def transform(self):
        logger.debug("Running transformations...")
        self._trim_df()
        self.data = self.data.select("loss_proof").distinct()
        self.data = self.data.withColumnRenamed("loss_proof", "proof")
        if self.metadata:
            pass

    def load(self, path: Optional[Union[Path, str]] = None, table: Optional[str] = None):
        persist_data(self, out_path=path, db_table=table)

    def _trim_df(self):
        self.data = trim_df(self.data)


class OryxLossesSummary(ETL):
    def __init__(self, source: Union[Path, str], spark: SparkSession,
                 metadata: Optional = None, db_conn: Optional = None):
        super().__init__(source, spark)
        self.metadata = metadata
        self.db = db_conn

    def extract(self):
        logger.info(f"Extracting data from {self.source}")
        self.data = self.spark_session.read.option("header", "true").option("inferSchema", "true").csv(self.source)

    def transform(self):
        logger.debug("Running transformations...")
        self._trim_df()
        self._filter_base_cols()
        self._extract_data()
        self._filter_final_cols()
        self._fill_na_with_null()
        if self.metadata:
            self.data = add_metadata(self.data, self.metadata, leftside_insert=True)

    def load(self, path: Optional[Union[Path, str]] = None, table: Optional[str] = None):
        persist_data(self, out_path=path, db_table=table)

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
        self.data = self.data.select("category_name", "destroyed",
                                     "damaged", "abandoned", "captured", "total")

    def _trim_df(self):
        self.data = trim_df(self.data)


class OryxLossesCategories(OryxLossesProofs):
    def __init__(self, source: Union[Path, str], spark: SparkSession,
                 metadata: Optional = None, db_conn: Optional = None):
        super().__init__(source, spark, metadata, db_conn)

    def extract(self):
        super().extract()

    def transform(self):
        logger.debug("Running transformations...")
        self._trim_df()
        self.data = self.data.select("category_name").distinct()
        self.data = self.data.withColumnRenamed("category_name", "category")
        df = self.data.toPandas()
        if self.metadata:
            pass

    def load(self, path: Optional[Union[Path, str]] = None, table: Optional[str] = None):
        super().load(path=path, table=table)

# Classes for SCD type two update

class OryxLossesItemSCD2(OryxLossesItem):
    """Enhanced version of OryxLossesItem that uses start_date and stop_date instead of as_of for record keeping.
    As a result, db_conn is no longer optional here
    Compares parsed data with db:
    - removes old data by adding as_of as stop_date
    - adds new values by insertion with as_of as start_date
    - leaves other data unchanged
    """

    def __init__(self, source: Union[Path, str], spark: SparkSession,
                 db_conn: Union[Path, str],
                 metadata: Optional = None):
        super().__init__(source, spark, metadata=metadata, db_conn=db_conn)
        self.category_keys = {}

    def extract(self):
        super().extract()
        if self.metadata:
            self.category_keys = self._get_category_keys()

    def transform(self):
        super().transform()
        if self.category_keys:
            self._replace_category_with_keys()

    def load(self, path: Optional[Union[Path, str]] = None, table: Optional[str] = None):
        persist_data_scd2(self, out_path=path, db_table=table, filter_columns=["party"])

    def _get_category_keys(self) -> dict:
        col_name = "category_name"
        with self.db as db_connection:
            categories_and_keys = db_connection.fetch_unique_data(self.data, col_name, "category_names", "category")
        logger.debug(f"Fetched {len(categories_and_keys)} categories for look-up.")
        categories_and_keys = {cat: key for key, cat in categories_and_keys}
        logger.info(categories_and_keys)
        return categories_and_keys

    def _replace_category_with_keys(self):
        mapping = create_map([lit(x) for x in chain(*self.category_keys.items())])
        self.data = self.data.withColumn("category_id", mapping[self.data["category_name"]])
        self.data = self.data.drop("category_name")


class OryxLossesSummarySCD2(OryxLossesSummary):
    def __init__(self, source: Union[Path, str], spark: SparkSession,
                 db_conn: Union[Path, str],
                 metadata: Optional = None):
        super().__init__(source, spark, metadata=metadata, db_conn=db_conn)

    def extract(self):
        super().extract()

    def transform(self):
        super().transform()

    def load(self, path: Optional[Union[Path, str]] = None, table: Optional[str] = None):
        persist_data_scd2(self, out_path=path, db_table=table, filter_columns=["party"])


ETLCLASSES = {"OryxLossesSummary": OryxLossesSummary,
              "OryxLossesItem": OryxLossesItem,
              "OryxLossesProofs": OryxLossesProofs,
              "OryxLossesItemSCD2": OryxLossesItemSCD2,
              "OryxLossesSummarySCD2": OryxLossesSummarySCD2,
              "OryxLossesCategories": OryxLossesCategories}
