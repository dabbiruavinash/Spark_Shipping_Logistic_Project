from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from core.base_processor import BaseProcessor

class PortPerformanceDashboard(BaseProcessor):
    """
    Module: 10.1 - Aggregates port turnaround and efficiency KPIs
    Metadata:
      - Version: 1.2
      - KPIs: 15+ port efficiency metrics
    """
    
    def calculate_port_kpis(self, df: DataFrame) -> DataFrame:
        """Calculate port performance KPIs"""
        
        return df.groupBy("port_code", "year", "month") \
            .agg(
                F.count("*").alias("vessel_visits"),
                F.avg("turnaround_time_hours").alias("avg_turnaround_time"),
                F.percentile_approx("turnaround_time_hours", 0.5).alias("median_turnaround_time"),
                F.avg("berth_utilization").alias("avg_berth_utilization"),
                F.sum("cargo_throughput_teu").alias("total_throughput_teu"),
                F.avg("gang_productivity").alias("avg_gang_productivity"),
                F.sum("demurrage_charges").alias("total_demurrage_charges")
            ) \
            .withColumn("efficiency_rating",
                       F.when(F.col("avg_turnaround_time") < 24, "EXCELLENT")
                        .when(F.col("avg_turnaround_time") < 48, "GOOD")
                        .when(F.col("avg_turnaround_time") < 72, "AVERAGE")
                        .otherwise("POOR")) \
            .withColumn("throughput_per_berth",
                       F.col("total_throughput_teu") / F.col("berth_count")) \
            .withColumn("rank_by_efficiency",
                       F.row_number().over(
                           Window.partitionBy("year", "month")
                                .orderBy(F.col("avg_turnaround_time"))
                       ))