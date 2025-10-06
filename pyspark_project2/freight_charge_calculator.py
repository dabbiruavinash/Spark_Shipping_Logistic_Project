from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType
from core.base_processor import BaseProcessor

class FreightChargeCalculator(BaseProcessor):
    """
    Module: 7.1 - Calculates total shipping charges
    Metadata:
      - Version: 1.1
      - Components: Base freight, Surcharges, Accessorials
    """
    
    def calculate_freight_charges(self, df: DataFrame) -> DataFrame:
        """Calculate comprehensive freight charges"""
        
        return df \
            .withColumn("base_freight", 
                       F.col("quantity") * F.col("rate_per_unit")) \
            .withColumn("bunker_surcharge", 
                       F.col("base_freight") * F.col("baf_rate")) \
            .withColumn("currency_surcharge", 
                       F.col("base_freight") * F.col("caf_rate")) \
            .withColumn("port_congestion_surcharge",
                       F.col("base_freight") * F.col("pcs_rate")) \
            .withColumn("peak_season_surcharge",
                       F.col("base_freight") * F.col("pss_rate")) \
            .withColumn("total_surcharges",
                       F.col("bunker_surcharge") + F.col("currency_surcharge") + 
                       F.col("port_congestion_surcharge") + F.col("peak_season_surcharge")) \
            .withColumn("total_freight_charge",
                       (F.col("base_freight") + F.col("total_surcharges"))
                       .cast(DecimalType(15, 2)))