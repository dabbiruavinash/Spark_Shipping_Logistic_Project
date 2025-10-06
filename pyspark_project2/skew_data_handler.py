from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class SkewDataHandler(BaseProcessor):
    """
    Module: 8.3 - Redistributes skewed route and vessel data
    Metadata:
      - Version: 1.2
      - Skew Handling: Salting, Repartitioning
    """
    
    def handle_skew_with_salting(self, df: DataFrame, key_column: str, salt_buckets: int = 10) -> DataFrame:
        """Handle data skew using salting technique"""
        
        return df \
            .withColumn("salt", (F.rand() * salt_buckets).cast("int")) \
            .withColumn("salted_key", F.concat(F.col(key_column), F.lit("_"), F.col("salt")))
    
    def optimize_repartitioning(self, df: DataFrame, partition_columns: list) -> DataFrame:
        """Optimize repartitioning for skewed data"""
        
        return df.repartition(*partition_columns)
    
    def detect_skew(self, df: DataFrame, key_column: str) -> dict:
        """Detect data skew in DataFrame"""
        
        skew_stats = df.groupBy(key_column).count() \
            .select(
                F.avg("count").alias("avg_count"),
                F.stddev("count").alias("stddev_count"),
                F.max("count").alias("max_count"),
                F.min("count").alias("min_count")
            ).first()
        
        return {
            "skew_ratio": skew_stats["max_count"] / skew_stats["avg_count"] if skew_stats["avg_count"] > 0 else 0,
            "max_count": skew_stats["max_count"],
            "avg_count": skew_stats["avg_count"]
        }