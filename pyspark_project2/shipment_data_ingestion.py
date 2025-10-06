from pyspark.sql import DataFrame, functions as F
from typing import Dict
from core.base_processor import BaseProcessor
from core.data_models import ShippingSchemas

class ShipmentDataIngestion(BaseProcessor):
    """
    Module: 1.2 - Ingests shipment manifests and order data
    Metadata:
      - Version: 1.0
      - Input Sources: ERP System, Booking Portals
      - Output: Standardized shipment records
      - Update Frequency: Real-time + Batch
    """
    
    def ingest_shipment_manifests(self, manifest_path: str) -> DataFrame:
        """Ingest shipment manifest data"""
        manifests = self.spark.read.format("parquet").load(manifest_path)
        
        return manifests \
            .withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("data_source", F.lit("manifest_system")) \
            .withColumn("record_hash", F.sha2(F.concat_ws("|", *manifests.columns), 256))
    
    def ingest_order_data(self, order_path: str) -> DataFrame:
        """Ingest customer order data"""
        orders = self.spark.read.format("delta").load(order_path)
        
        return orders \
            .withColumn("order_total_value", 
                       F.col("quantity") * F.col("unit_price")) \
            .withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("data_source", F.lit("order_management_system"))
    
    def merge_shipment_sources(self, manifests_df: DataFrame, orders_df: DataFrame) -> DataFrame:
        """Merge multiple shipment data sources"""
        return manifests_df.alias("m").join(
            orders_df.alias("o"),
            F.col("m.shipment_id") == F.col("o.shipment_id"),
            "left_outer"
        ).select(
            F.coalesce(F.col("m.shipment_id"), F.col("o.shipment_id")).alias("shipment_id"),
            F.col("m.vessel_id"),
            F.col("m.container_id"),
            F.col("o.customer_id"),
            F.col("o.order_total_value"),
            F.col("m.origin_port"),
            F.col("m.destination_port"),
            F.col("m.estimated_departure"),
            F.col("m.estimated_arrival"),
            F.col("m.ingestion_timestamp")
        )