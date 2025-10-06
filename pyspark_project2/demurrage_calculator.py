from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType
from core.base_processor import BaseProcessor

class DemurrageCalculator(BaseProcessor):
    """
    Module: 4.3 - Computes port delay penalties
    Metadata:
      - Version: 1.0
      - Rules: Contract-specific demurrage terms
    """
    
    def calculate_demurrage_charges(self, df: DataFrame) -> DataFrame:
        """Calculate demurrage and detention charges"""
        
        return df \
            .withColumn("free_time_hours", F.lit(72)) \  # Standard free time
            .withColumn("actual_port_time_hours",
                       (F.unix_timestamp(F.col("departure_time")) - 
                        F.unix_timestamp(F.col("arrival_time"))) / 3600) \
            .withColumn("excess_time_hours",
                       F.greatest(F.col("actual_port_time_hours") - 
                                 F.col("free_time_hours"), 0)) \
            .withColumn("demurrage_days", 
                       F.ceil(F.col("excess_time_hours") / 24)) \
            .withColumn("demurrage_rate_per_day", 
                       F.when(F.col("vessel_type") == "CONTAINER", 10000)
                        .when(F.col("vessel_type") == "TANKER", 15000)
                        .when(F.col("vessel_type") == "BULK_CARRIER", 12000)
                        .otherwise(8000)) \
            .withColumn("total_demurrage_charge",
                       (F.col("demurrage_days") * F.col("demurrage_rate_per_day"))
                       .cast(DecimalType(15, 2)))