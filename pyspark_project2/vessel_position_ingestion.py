from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from core.base_processor import BaseProcessor

class VesselPositionIngestion(BaseProcessor):
    """
    Module: 1.3 - Streams vessel GPS and AIS data in near real-time
    Metadata:
      - Version: 1.1
      - Data Source: AIS Receivers, Satellite Tracking
      - Latency: < 5 minutes
      - Volume: 10K+ positions per hour
    """
    
    AIS_SCHEMA = StructType([
        StructField("vessel_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("destination", StringType(), True),
        StructField("eta", TimestampType(), True)
    ])
    
    def stream_ais_data(self, kafka_config: Dict) -> DataFrame:
        """Stream AIS data from Kafka"""
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .options(**kafka_config) \
            .load()
        
        # Parse JSON AIS messages
        return raw_stream.select(
            F.from_json(F.col("value").cast("string"), self.AIS_SCHEMA).alias("data")
        ).select("data.*") \
         .withColumn("ingestion_time", F.current_timestamp()) \
         .withWatermark("timestamp", "10 minutes")
    
    def batch_load_historical_positions(self, historical_path: str) -> DataFrame:
        """Load historical vessel positions"""
        return self.spark.read.format("parquet") \
            .schema(self.AIS_SCHEMA) \
            .load(historical_path) \
            .withColumn("data_source", F.lit("historical_ais"))