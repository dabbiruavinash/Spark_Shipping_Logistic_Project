from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F

class RouteTransformer(BaseProcessor):
    def calculate_route_efficiency(self, df: DataFrame) -> DataFrame:
        route_metrics = df.groupBy("origin_port", "destination_port").agg(
            F.count("*").alias("route_volume"),
            F.avg("transit_days").alias("avg_route_transit_time"),
            F.stddev("transit_days").alias("route_transit_stddev"),
            F.avg(F.when(F.col("is_delayed"), 1).otherwise(0)).alias("route_delay_rate")
        )
        
        return route_metrics.withColumn("route_efficiency_score",
            (1 - F.col("route_delay_rate")) * 0.6 +
            (1 / F.col("avg_route_transit_time")) * 0.4
        )
    
    def identify_popular_routes(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        return df.groupBy("origin_port", "destination_port") \
            .agg(F.count("*").alias("shipment_count")) \
            .orderBy(F.desc("shipment_count")) \
            .limit(top_n)