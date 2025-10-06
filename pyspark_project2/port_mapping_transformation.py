from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class PortMappingTransformation(BaseProcessor):
    """
    Module: 2.3 - Maps port codes to standardized master data
    Metadata:
      - Version: 1.0
      - Reference: UN/LOCODE standard
      - Coverage: 5,000+ ports worldwide
    """
    
    def map_port_codes(self, df: DataFrame, port_master_df: DataFrame) -> DataFrame:
        """Map port codes to standardized names and locations"""
        return df.alias("main") \
            .join(
                F.broadcast(port_master_df.alias("ports")),
                (F.col("main.port_code") == F.col("ports.unlocode")) |
                (F.col("main.port_code") == F.col("ports.port_code")),
                "left_outer"
            ).select(
                "main.*",
                F.coalesce(F.col("ports.port_name"), F.col("main.port_code")).alias("standard_port_name"),
                F.col("ports.country_code"),
                F.col("ports.latitude"),
                F.col("ports.longitude"),
                F.col("ports.timezone")
            )
    
    def validate_port_codes(self, df: DataFrame, valid_ports_df: DataFrame) -> DataFrame:
        """Validate port codes against master list"""
        valid_ports = valid_ports_df.select("unlocode").rdd.flatMap(lambda x: x).collect()
        
        return df.withColumn("is_valid_port", 
            F.col("port_code").isin(valid_ports)) \
            .withColumn("validation_status",
                F.when(F.col("is_valid_port"), "VALID")
                 .otherwise("INVALID"))