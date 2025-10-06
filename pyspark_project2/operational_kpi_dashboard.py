from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from core.base_processor import BaseProcessor

class OperationalKPIDashboard(BaseProcessor):
    """
    Module: 10.5 - Displays global KPIs like on-time %, cost, and route delays
    Metadata:
      - Version: 1.3
      - KPIs: 20+ operational metrics
    """
    
    def calculate_operational_kpis(self, df: DataFrame) -> DataFrame:
        """Calculate comprehensive operational KPIs"""
        
        return df.groupBy("trade_lane", "quarter") \
            .agg(
                # On-time performance
                F.avg(F.when(F.col("is_on_time"), 1).otherwise(0)).alias("on_time_performance"),
                
                # Cost efficiency
                F.avg(F.col("total_cost_per_teu")).alias("avg_cost_per_teu"),
                F.avg(F.col("fuel_cost_per_mile")).alias("avg_fuel_cost_per_mile"),
                
                # Operational efficiency
                F.avg(F.col("vessel_utilization")).alias("avg_vessel_utilization"),
                F.avg(F.col("port_turnaround_time")).alias("avg_port_turnaround"),
                
                # Reliability
                F.stddev(F.col("transit_time")).alias("transit_time_variability"),
                F.avg(F.col("delay_hours")).alias("avg_delay_hours"),
                
                # Volume metrics
                F.sum(F.col("teu_volume")).alias("total_teu_volume"),
                F.count("*").alias("voyage_count")
            ) \
            .withColumn("reliability_score",
                       (100 - F.col("transit_time_variability")) * 
                       F.col("on_time_performance")) \
            .withColumn("efficiency_score",
                       (100 / F.col("avg_cost_per_teu")) * 
                       F.col("avg_vessel_utilization")) \
            .withColumn("overall_performance_score",
                       (F.col("reliability_score") * 0.4 + 
                        F.col("efficiency_score") * 0.3 + 
                        F.col("on_time_performance") * 100 * 0.3)) \
            .withColumn("performance_tier",
                       F.when(F.col("overall_performance_score") > 80, "TIER_1")
                        .when(F.col("overall_performance_score") > 60, "TIER_2")
                        .when(F.col("overall_performance_score") > 40, "TIER_3")
                        .otherwise("TIER_4"))