from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast
from core.base_processor import BaseProcessor

class BroadcastJoinOptimizer(BaseProcessor):
    """
    Module: 8.2 - Manages small lookup table joins
    Metadata:
      - Version: 1.1
      - Threshold: 10MB for broadcast
    """
    
    def optimize_joins(self, large_df: DataFrame, small_df: DataFrame, join_keys: list) -> DataFrame:
        """Optimize joins using broadcast for small tables"""
        
        return large_df.join(broadcast(small_df), join_keys)
    
    def auto_detect_broadcast(self, df: DataFrame, size_threshold_mb: int = 10) -> bool:
        """Auto-detect if DataFrame should be broadcast"""
        
        # Estimate DataFrame size
        estimated_size = df.count() * len(df.columns) * 100  # Rough estimation
        return estimated_size < (size_threshold_mb * 1024 * 1024)