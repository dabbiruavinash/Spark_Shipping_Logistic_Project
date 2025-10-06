from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class PortEventStreamHandler(BaseProcessor):
    """
    Module: 6.2 - Ingests live port entry and exit events
    Metadata:
      - Version: 1.1
      - Events: Arrival, Departure, Berthing
    """
    
    def process_port_events(self, stream_df: DataFrame) -> DataFrame:
        """Process port event stream"""
        
        return stream_df \
            .withColumn("event_timestamp", F.to_timestamp(F.col("event_time"))) \
            .withColumn("event_type_category",
                       F.when(F.col("event_type").isin(["ARRIVAL", "DEPARTURE"]), "MOVEMENT")
                        .when(F.col("event_type").isin(["BERTHING", "UNBERTHING"]), "BERTH")
                        .otherwise("OTHER")) \
            .withColumn("port_stay_duration",
                       F.when(F.col("event_type") == "DEPARTURE",
                             F.unix_timestamp(F.col("event_timestamp")) - 
                             F.unix_timestamp(F.col("arrival_timestamp"))) / 3600) \
            .withWatermark("event_timestamp", "10 minutes")