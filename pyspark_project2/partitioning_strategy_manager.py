from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class PartitioningStrategyManager(BaseProcessor):
    """
    Module: 8.1 - Defines partitioning (e.g., by year/month/port)
    Metadata:
      - Version: 1.2
      - Strategies: Time-based, Geographic, Business
    """
    
    def apply_time_based_partitioning(self, df: DataFrame) -> DataFrame:
        """Apply time-based partitioning"""
        
        return df \
            .withColumn("year", F.year(F.col("event_timestamp"))) \
            .withColumn("month", F.month(F.col("event_timestamp"))) \
            .withColumn("day", F.dayofmonth(F.col("event_timestamp")))
    
    def apply_business_partitioning(self, df: DataFrame) -> DataFrame:
        """Apply business-based partitioning"""
        
        return df \
            .withColumn("trade_lane", 
                       F.concat(F.col("origin_region"), F.lit("_"), F.col("dest_region"))) \
            .withColumn("vessel_size_category",
                       F.when(F.col("teu_capacity") > 18000, "ULCV")
                        .when(F.col("teu_capacity") > 8000, "VLCS")
                        .when(F.col("teu_capacity") > 5000, "PANAMAX")
                        .otherwise("FEEDER"))