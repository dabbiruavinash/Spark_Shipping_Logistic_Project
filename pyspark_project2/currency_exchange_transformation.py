from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType
from core.base_processor import BaseProcessor

class CurrencyExchangeTransformation(BaseProcessor):
    """
    Module: 2.4 - Converts freight cost into standard currency
    Metadata:
      - Version: 1.1
      - Base Currency: USD
      - Update Frequency: Daily
    """
    
    def convert_to_base_currency(self, df: DataFrame, exchange_rates_df: DataFrame) -> DataFrame:
        """Convert all amounts to base currency (USD)"""
        latest_rates = exchange_rates_df \
            .withColumn("rank", F.row_number().over(
                Window.partitionBy("from_currency").orderBy(F.col("effective_date").desc())
            )) \
            .filter(F.col("rank") == 1) \
            .drop("rank")
        
        return df.alias("txn") \
            .join(
                F.broadcast(latest_rates.alias("rates")),
                F.col("txn.currency") == F.col("rates.from_currency"),
                "left_outer"
            ).withColumn("amount_usd",
                F.when(F.col("txn.currency") == "USD", F.col("txn.amount"))
                 .otherwise(F.col("txn.amount") * F.coalesce(F.col("rates.exchange_rate"), F.lit(1.0)))
            ).withColumn("amount_usd", 
                F.col("amount_usd").cast(DecimalType(15, 2)))