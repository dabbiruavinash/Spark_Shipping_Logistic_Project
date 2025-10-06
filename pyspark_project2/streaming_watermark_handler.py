from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class StreamingWatermarkHandler(BaseProcessor):
    """
    Module: 6.4 - Handles late data and watermarking
    Metadata:
      - Version: 1.1
      - Late Data: Up to 2 hours
    """
    
    def handle_late_data(self, stream_df: DataFrame, watermark_duration: str = "2 hours") -> DataFrame:
        """Handle late arriving data with watermarks"""
        
        return stream_df \
            .withWatermark("event_timestamp", watermark_duration) \
            .dropDuplicates(["event_id", "event_timestamp"]) \
            .withColumn("is_late_data", 
                       F.col("processing_timestamp") > 
                       (F.col("event_timestamp") + F.expr("INTERVAL 1 HOUR")))