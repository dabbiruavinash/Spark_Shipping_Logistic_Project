from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, StringType
from core.base_processor import BaseProcessor

class RouteDataTransformation(BaseProcessor):
    """
    Module: 2.2 - Standardizes route data using explode and flatten operations
    Metadata:
      - Version: 1.1
      - Operations: explode, flatten, array operations
    """
    
    def explode_route_waypoints(self, df: DataFrame) -> DataFrame:
        """Explode route waypoints array into individual rows"""
        return df.withColumn("waypoint", F.explode(F.col("route_waypoints"))) \
                 .select("*", "waypoint.*") \
                 .drop("route_waypoints")
    
    def flatten_nested_route_data(self, df: DataFrame) -> DataFrame:
        """Flatten nested route JSON structures"""
        return df.select(
            "route_id",
            "vessel_id",
            F.col("route_info.origin").alias("origin_port"),
            F.col("route_info.destination").alias("destination_port"),
            F.col("route_info.waypoints").alias("intermediate_ports"),
            F.col("route_info.total_distance").alias("total_distance_nm"),
            F.col("metadata.created_by").alias("route_creator"),
            F.col("metadata.created_at").alias("route_created_date")
        )
    
    def calculate_route_segments(self, df: DataFrame) -> DataFrame:
        """Calculate individual route segments"""
        return df.withColumn("route_segments", 
            F.transform(F.col("intermediate_ports"), 
                       lambda port: F.struct(port.alias("port_code")))) \
            .withColumn("segment_count", F.size(F.col("intermediate_ports")) + 1)