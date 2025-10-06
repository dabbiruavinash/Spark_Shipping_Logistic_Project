from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable
from core.base_processor import BaseProcessor

class HistoricalSnapshotHandler(BaseProcessor):
    """
    Module: 5.5 - Enables time travel for historical analysis
    Metadata:
      - Version: 1.1
      - Retention: 7 years for compliance
    """
    
    def create_snapshot(self, df: DataFrame, snapshot_path: str, snapshot_date: str):
        """Create historical snapshot"""
        
        df.withColumn("snapshot_date", F.lit(snapshot_date)) \
          .write \
          .format("delta") \
          .mode("overwrite") \
          .option("delta.appendOnly", "true") \
          .save(snapshot_path)
    
    def get_snapshot_as_of_date(self, snapshot_path: str, target_date: str) -> DataFrame:
        """Get snapshot as of specific date"""
        
        return self.spark.read \
            .format("delta") \
            .option("timestampAsOf", target_date) \
            .load(snapshot_path)