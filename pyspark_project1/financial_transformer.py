from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F, Window

class FinancialTransformer(BaseProcessor):
    def calculate_revenue_metrics(self, shipments_df: DataFrame, carriers_df: DataFrame) -> DataFrame:
        # Assuming carrier rates are available
        revenue_df = shipments_df.join(carriers_df, "carrier_id", "left") \
            .withColumn("estimated_revenue", F.col("weight_kg") * F.col("rate_per_kg"))
        
        return revenue_df.groupBy("customer_id", "shipment_month") \
            .agg(
                F.sum("estimated_revenue").alias("monthly_revenue"),
                F.avg("estimated_revenue").alias("avg_shipment_value"),
                F.count("*").alias("shipment_count")
            ) \
            .withColumn("revenue_growth", 
                       F.col("monthly_revenue") - F.lag("monthly_revenue").over(
                           Window.partitionBy("customer_id").orderBy("shipment_month")))