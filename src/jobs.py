"""
Actual job definition, preferably based on abstract class from sparkutil
"""

from src.sparkutil import ETL, ETLJob, trim_df

ETLJOBS = [OryxLossesItem, OryxLossesProofs, OryxLossesSummary]


# etl executor
class ETLExecutor:
    def __init__(self):
        raise NotImplemented

    # method to load ETL jobs

    # method to run single ETL job

    # method to run all ETL jobs one after another


class OryxLossesItem(ETL):
    def __init__(self, source, spark):
        super().__init__(source, spark)

    def extract(self):
        self.data = spark.read.option("header", "true").option("inferSchema", "true").csv(self.source)

    def transform(self):
        self._trim_df()
        self._filter_base_cols()
        self._build_cleaned_items()
        self._split_to_losses()
        self._split_loss_to_rows()
        self._filter_final_cols()

    def load(self, path: Union[Path, str]):
        self.data.write.option("header", True).mode("overwrite").csv(path)

    def _trim_df(self):
        self.data = trim_df(self.data)

    def _filter_base_cols(self):
        self.data = self.data.select("category_name", "type_name", "type_ttl_count", "loss_item")

    def _build_cleaned_items(self):
        """
        Moving descriptions with multiple losses into more processable, comma delimited text
        """
        self.data = (self.data.withColumn("loss_item", regexp_replace(self.data["loss_item"], r"[()]", ""))
                     .withColumn("cleaned_items", regexp_replace(self.data["loss_item"], "and", ","))
                     .withColumn("cleaned_items", regexp_replace(self.data["cleaned_items"], r"\b(and)\b", "")))

    def _split_to_losses(self):
        """

        """
        self.data = (self.data.withColumn("loss_id", split(self.data["cleaned_items"], ",\s*"))
                     .withColumn("loss_type", regexp_replace(self.data["loss_item"], r".*?,\s*", "")))

    def _split_loss_to_rows(self):
        """
        Separating ids into multiple rows and removing
        remaining invalid rows (removing those without integer id)
        """
        self.data = (self.data.withColumn("loss_id", explode(self.data["loss_id"]))
                     .withColumn("loss_id", col("loss_id").cast("int"))
                     .filter(col("loss_id").isNotNull()))

    def _filter_final_cols(self):
        self.data = self.data.select("category_name", "type_name", "loss_item",
                                     "cleaned_items", "loss_id", "loss_type")


class OryxLossesProofs(ETL):
    def __init__(self, source, spark):
        super().__init__(source, spark)

    def extract(self):
        self.data = spark.read.option("header", "true").option("inferSchema", "true").csv(self.source)

    def transform(self):
        self._trim_df()
        self.data = self.data.select("loss_proof").distinct()

    def load(self, path: Union[Path, str]):
        self.data.write.option("header", True).mode("overwrite").csv(path)

    def _trim_df(self):
        self.data = trim_df(self.data)


class OryxLossesSummary(ETL):
    def __init__(self):
        raise NotImplemented
