from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, StringType
from core.base_processor import BaseProcessor

class RouteOptimizationEngine(BaseProcessor):
    """
    Module: 3.5 - Suggests optimized paths using cost and time metrics
    Metadata:
      - Version: 1.2
      - Optimization: Cost, Time, Emissions
      - Algorithm: Multi-objective optimization
    """
    
    def optimize_routes(self, df: DataFrame) -> DataFrame:
        """Optimize routes based on multiple factors"""
        
        return df \
            .withColumn("cost_score", 
                       (F.col("fuel_cost") + F.col("port_charges") + F.col("canal_dues")) / F.col("distance_nm")) \
            .withColumn("time_score", F.col("estimated_duration_hours") / F.col("distance_nm")) \
            .withColumn("emission_score", F.col("co2_per_teu")) \
            .withColumn("composite_score",
                       (F.col("cost_score") * 0.5 + 
                        F.col("time_score") * 0.3 + 
                        F.col("emission_score") * 0.2)) \
            .withColumn("optimization_rank",
                       F.row_number().over(
                           Window.partitionBy("origin_port", "destination_port")
                                .orderBy(F.col("composite_score"))
                       )) \
            .withColumn("is_optimized_route", F.col("optimization_rank") == 1)