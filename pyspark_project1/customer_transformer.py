from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F, Window

class CustomerTransformer(BaseProcessor):
    def create_customer_lifetime_value(self, shipments_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        customer_shipment_metrics = shipments_df.groupBy("customer_id").agg(
            F.count("*").alias("total_shipments"),
            F.sum("weight_kg").alias("total_weight_shipped"),
            F.avg("transit_days").alias("avg_transit_time"),
            F.avg(F.when(F.col("is_delayed"), 1).otherwise(0)).alias("avg_delay_rate")
        )
        
        return customers_df.join(customer_shipment_metrics, "customer_id", "left") \
            .withColumn("clv_score", 
                       F.col("total_shipments") * 0.4 + 
                       (F.col("credit_limit") / 1000) * 0.3 + 
                       (1 - F.col("avg_delay_rate")) * 0.3)
    
    def segment_customers(self, df: DataFrame) -> DataFrame:
        return df.withColumn("customer_tier",
            F.when(F.col("clv_score") > 0.8, "Platinum")
             .when(F.col("clv_score") > 0.6, "Gold")
             .when(F.col("clv_score") > 0.4, "Silver")
             .otherwise("Bronze")
        )