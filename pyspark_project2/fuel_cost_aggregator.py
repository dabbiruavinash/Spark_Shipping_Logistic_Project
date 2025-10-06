from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from core.base_processor import BaseProcessor

class FuelCostAggregator(BaseProcessor):
    """
    Module: 7.2 - Aggregates cost by region, vessel type, and time
    Metadata:
      - Version: 1.0
      - Aggregation: Multiple dimensions
    """
    
    def aggregate_fuel_costs(self, df: DataFrame) -> DataFrame:
        """Aggregate fuel costs by multiple dimensions"""
        
        return df.groupBy("region", "vessel_type", "year", "month") \
            .agg(
                F.sum("fuel_consumption").alias("total_fuel_consumption"),
                F.sum("fuel_cost").alias("total_fuel_cost"),
                F.avg("fuel_cost_per_nautical_mile").alias("avg_fuel_cost_per_mile"),
                F.count("*").alias("voyage_count")
            ) \
            .withColumn("fuel_cost_per_teu",
                       F.col("total_fuel_cost") / F.col("total_teu")) \
            .withColumn("month_over_month_growth",
                       (F.col("total_fuel_cost") - F.lag("total_fuel_cost").over(
                           Window.partitionBy("region", "vessel_type")
                                .orderBy("year", "month")
                       )) / F.lag("total_fuel_cost").over(
                           Window.partitionBy("region", "vessel_type")
                                .orderBy("year", "month")
                       ) * 100)