from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F, Window
from services.window_service import WindowService

class ShipmentTransformer(BaseProcessor):
    def __init__(self, spark, config):
        super().__init__(spark, config)
        self.window_service = WindowService(spark, config)
    
    def calculate_shipment_metrics(self, df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("customer_id").orderBy("shipment_date")
        
        return df \
            .withColumn("prev_shipment_date", F.lag("shipment_date").over(window_spec)) \
            .withColumn("days_between_shipments", 
                       F.datediff(F.col("shipment_date"), F.col("prev_shipment_date"))) \
            .withColumn("shipment_sequence", F.row_number().over(window_spec)) \
            .withColumn("total_customer_shipments", F.count("*").over(Window.partitionBy("customer_id")))
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.clustering import KMeans
        
        feature_cols = ["weight_kg", "volume_cbm", "transit_days"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        assembled_df = assembler.transform(df)
        
        kmeans = KMeans(k=3, seed=42, featuresCol="features")
        model = kmeans.fit(assembled_df)
        
        return model.transform(assembled_df) \
            .withColumnRenamed("prediction", "cluster") \
            .drop("features")
    
    def create_shipment_summary(self, df: DataFrame) -> DataFrame:
        return df.groupBy("customer_id", "shipment_month") \
            .agg(
                F.count("*").alias("monthly_shipments"),
                F.sum("weight_kg").alias("total_weight"),
                F.avg("transit_days").alias("avg_transit_days"),
                F.sum(F.when(F.col("is_delayed"), 1).otherwise(0)).alias("delayed_shipments")
            ) \
            .withColumn("delivery_success_rate", 
                       (F.col("monthly_shipments") - F.col("delayed_shipments")) / F.col("monthly_shipments"))