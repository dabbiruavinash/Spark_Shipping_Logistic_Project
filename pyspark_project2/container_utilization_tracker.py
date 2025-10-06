from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class ContainerUtilizationTracker(BaseProcessor):
    """
    Module: 4.4 - Tracks space usage and load factors
    Metadata:
      - Version: 1.1
      - Metrics: TEU utilization, Weight utilization
    """
    
    def track_container_utilization(self, df: DataFrame) -> DataFrame:
        """Track container utilization metrics"""
        
        return df \
            .withColumn("teu_capacity", 
                       F.when(F.col("vessel_size") == "ULCV", 24000)
                        .when(F.col("vessel_size") == "VLCS", 18000)
                        .when(F.col("vessel_size") == "PANAMAX", 5000)
                        .otherwise(3000)) \
            .withColumn("teu_utilization_rate",
                       (F.col("loaded_teu") / F.col("teu_capacity")) * 100) \
            .withColumn("weight_utilization_rate",
                       (F.col("total_weight") / F.col("max_weight_capacity")) * 100) \
            .withColumn("utilization_category",
                       F.when(F.col("teu_utilization_rate") > 90, "HIGH")
                        .when(F.col("teu_utilization_rate") > 75, "OPTIMAL")
                        .when(F.col("teu_utilization_rate") > 60, "MODERATE")
                        .otherwise("LOW")) \
            .withColumn("is_underutilized",
                       F.col("teu_utilization_rate") < 60)