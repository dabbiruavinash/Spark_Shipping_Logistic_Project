from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class VesselUtilizationReport(BaseProcessor):
    """
    Module: 10.2 - Reports vessel usage by region and period
    Metadata:
      - Version: 1.1
      - Metrics: Utilization rates, Idle time
    """
    
    def generate_utilization_report(self, df: DataFrame) -> DataFrame:
        """Generate vessel utilization report"""
        
        return df.groupBy("vessel_id", "vessel_type", "region", "quarter") \
            .agg(
                F.count("*").alias("voyage_count"),
                F.sum("distance_nm").alias("total_distance"),
                F.avg("teu_utilization_rate").alias("avg_teu_utilization"),
                F.avg("weight_utilization_rate").alias("avg_weight_utilization"),
                F.sum("at_sea_days").alias("total_sea_days"),
                F.sum("in_port_days").alias("total_port_days"),
                F.avg("fuel_consumption_per_day").alias("avg_daily_fuel_consumption")
            ) \
            .withColumn("total_operating_days",
                       F.col("total_sea_days") + F.col("total_port_days")) \
            .withColumn("utilization_rate",
                       (F.col("total_operating_days") / 90) * 100) \  # Quarterly basis
            .withColumn("productivity_teu_per_day",
                       F.col("total_teu") / F.col("total_operating_days")) \
            .withColumn("efficiency_category",
                       F.when(F.col("utilization_rate") > 85, "HIGH")
                        .when(F.col("utilization_rate") > 70, "MEDIUM")
                        .otherwise("LOW"))