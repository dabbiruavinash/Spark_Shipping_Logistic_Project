from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from core.base_processor import BaseProcessor

class ShipmentStatusUpdater(BaseProcessor):
    """
    Module: 4.1 - Maintains real-time shipment status
    Metadata:
      - Version: 1.3
      - Statuses: 15+ shipment states
      - Real-time: Yes
    """
    
    def update_shipment_status(self, shipments_df: DataFrame, events_df: DataFrame) -> DataFrame:
        """Update shipment status based on latest events"""
        
        # Get latest event for each shipment
        latest_events = events_df \
            .withColumn("event_rank",
                       F.row_number().over(
                           Window.partitionBy("shipment_id")
                                .orderBy(F.col("event_timestamp").desc())
                       )) \
            .filter(F.col("event_rank") == 1) \
            .select("shipment_id", "event_type", "event_timestamp")
        
        # Map event types to shipment status
        event_to_status_map = {
            "BOOKED": "CONFIRMED",
            "VESSEL_DEPARTED": "IN_TRANSIT",
            "PORT_ARRIVAL": "ARRIVED_AT_PORT",
            "CUSTOMS_CLEARED": "CLEARED",
            "DELIVERED": "COMPLETED"
        }
        
        status_expr = F.create_map([F.lit(x) for x in 
            sum(event_to_status_map.items(), ())])
        
        return shipments_df.alias("s") \
            .join(latest_events.alias("e"), "shipment_id", "left_outer") \
            .withColumn("current_status",
                       F.coalesce(status_expr.getItem(F.col("e.event_type")), 
                                 F.col("s.current_status"))) \
            .withColumn("status_update_time", 
                       F.coalesce(F.col("e.event_timestamp"), 
                                 F.col("s.status_update_time"))) \
            .drop("e.event_type", "e.event_timestamp")