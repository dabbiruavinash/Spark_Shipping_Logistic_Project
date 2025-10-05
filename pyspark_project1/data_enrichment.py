from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

class DataEnrichmentProcessor(BaseProcessor):
    def enrich_with_geographic_data(self, df: DataFrame) -> DataFrame:
        # Mock geographic data enrichment
        from utils.udf_service import UDFService
        udf_service = UDFService(self.spark, self.config)
        
        get_region_udf = F.udf(udf_service.get_region_from_port, StringType())
        
        return df \
            .withColumn("origin_region", get_region_udf(F.col("origin_port"))) \
            .withColumn("destination_region", get_region_udf(F.col("destination_port")))
    
    def enrich_with_customer_segment(self, df: DataFrame) -> DataFrame:
        return df.withColumn("customer_segment", 
            F.when(F.col("credit_limit") > 10000, "Premium")
             .when(F.col("credit_limit") > 5000, "Standard")
             .otherwise("Basic")
        )
    
    def enrich_with_carrier_performance(self, shipments_df: DataFrame, carriers_df: DataFrame) -> DataFrame:
        carrier_performance = shipments_df \
            .groupBy("carrier_id") \
            .agg(
                F.avg("transit_days").alias("avg_transit_days"),
                F.count("*").alias("total_shipments"),
                F.avg(F.when(F.col("is_delayed"), 1).otherwise(0)).alias("delay_rate")
            )
        
        return carriers_df.join(carrier_performance, "carrier_id", "left")