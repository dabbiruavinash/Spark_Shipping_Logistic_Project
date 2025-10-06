from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from core.base_processor import BaseProcessor

class VesselTrackingStreamProcessor(BaseProcessor):
    """
    Module: 6.1 - Processes live vessel data with checkpointing
    Metadata:
      - Version: 1.2
      - Latency: < 1 minute
      - Checkpointing: Yes
    """
    
    def process_vessel_stream(self, stream_df: DataFrame) -> DataFrame:
        """Process real-time vessel tracking stream"""
        
        return stream_df \
            .withColumn("processing_time", F.current_timestamp()) \
            .withColumn("speed_category",
                       F.when(F.col("speed") > 20, "HIGH")
                        .when(F.col("speed") > 10, "MEDIUM")
                        .otherwise("LOW")) \
            .withColumn("position_quality",
                       F.when(F.col("accuracy") < 0.001, "HIGH")
                        .when(F.col("accuracy") < 0.01, "MEDIUM")
                        .otherwise("LOW")) \
            .withWatermark("timestamp", "5 minutes")
    
    def write_stream_to_delta(self, stream_df: DataFrame, output_path: str):
        """Write stream to Delta table with checkpointing"""
        
        query = stream_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{output_path}/_checkpoints") \
            .option("mergeSchema", "true") \
            .start(output_path)
        
        return query