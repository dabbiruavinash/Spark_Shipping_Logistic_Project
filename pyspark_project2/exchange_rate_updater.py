from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from core.base_processor import BaseProcessor

class ExchangeRateUpdater(BaseProcessor):
    """
    Module: 7.5 - Updates foreign exchange rates for multi-currency shipments
    Metadata:
      - Version: 1.1
      - Sources: Central banks, Forex APIs
    """
    
    def update_exchange_rates(self, df: DataFrame, new_rates_df: DataFrame) -> DataFrame:
        """Update exchange rates with latest values"""
        
        # Get latest exchange rates
        latest_rates = new_rates_df \
            .withColumn("rate_rank",
                       F.row_number().over(
                           Window.partitionBy("from_currency", "to_currency")
                                .orderBy(F.col("effective_date").desc())
                       )) \
            .filter(F.col("rate_rank") == 1) \
            .drop("rate_rank")
        
        return df.alias("main") \
            .join(F.broadcast(latest_rates.alias("rates")),
                  (F.col("main.currency") == F.col("rates.from_currency")) &
                  (F.col("rates.to_currency") == "USD"),
                  "left_outer") \
            .withColumn("amount_usd",
                       F.coalesce(F.col("main.amount") * F.col("rates.exchange_rate"),
                                 F.col("main.amount"))) \
            .drop("rates.from_currency", "rates.to_currency", "rates.exchange_rate")