from pyspark.sql.streaming import DataStreamWriter
from core.base_processor import BaseProcessor

class StreamingToDeltaSink(BaseProcessor):
    """
    Module: 6.5 - Writes streaming data into Delta Lake
    Metadata:
      - Version: 1.0
      - Output: Delta tables
    """
    
    def create_delta_sink(self, stream_df: DataFrame, output_path: str, 
                         checkpoint_path: str) -> DataStreamWriter:
        """Create Delta sink for streaming data"""
        
        return stream_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", output_path) \
            .option("mergeSchema", "true")