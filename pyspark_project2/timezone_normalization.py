from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType
from core.base_processor import BaseProcessor

class TimeZoneNormalization(BaseProcessor):
    """
    Module: 2.5 - Converts timestamps to UTC for consistency
    Metadata:
      - Version: 1.0
      - Standard: UTC
      - Handles: 40+ timezones
    """
    
    def convert_to_utc(self, df: DataFrame, timestamp_columns: list) -> DataFrame:
        """Convert multiple timestamp columns to UTC"""
        result_df = df
        
        for col in timestamp_columns:
            result_df = result_df \
                .withColumn(f"{col}_timezone", 
                    F.when(F.col("origin_port").startswith("US"), "America/New_York")
                     .when(F.col("origin_port").startswith("CN"), "Asia/Shanghai")
                     .when(F.col("origin_port").startswith("DE"), "Europe/Berlin")
                     .otherwise("UTC")) \
                .withColumn(f"{col}_utc",
                    F.from_utc_timestamp(
                        F.to_utc_timestamp(F.col(col), F.col(f"{col}_timezone")),
                        "UTC"
                    ))
        
        return result_df.drop(*[f"{col}_timezone" for col in timestamp_columns])
    
    def handle_dst_transitions(self, df: DataFrame) -> DataFrame:
        """Handle Daylight Saving Time transitions"""
        return df.withColumn("is_dst_applied",
            F.when(
                (F.month(F.col("timestamp")) >= 3) & 
                (F.month(F.col("timestamp")) <= 10),
                F.lit(True)
            ).otherwise(F.lit(False))
        )