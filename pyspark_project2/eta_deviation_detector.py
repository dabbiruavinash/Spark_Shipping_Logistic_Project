from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class ETADeviationDetector(BaseProcessor):
    """
    Module: 4.2 - Flags shipments with significant ETA delays
    Metadata:
      - Version: 1.1
      - Threshold: 24 hours for major deviations
    """
    
    def detect_eta_deviations(self, df: DataFrame) -> DataFrame:
        """Detect significant ETA deviations"""
        
        return df \
            .withColumn("actual_vs_planned_hours",
                       F.abs(F.unix_timestamp(F.col("actual_arrival")) - 
                            F.unix_timestamp(F.col("planned_arrival"))) / 3600) \
            .withColumn("deviation_category",
                       F.when(F.col("actual_vs_planned_hours") > 72, "SEVERE")
                        .when(F.col("actual_vs_planned_hours") > 48, "HIGH")
                        .when(F.col("actual_vs_planned_hours") > 24, "MEDIUM")
                        .when(F.col("actual_vs_planned_hours") > 12, "LOW")
                        .otherwise("MINIMAL")) \
            .withColumn("requires_alert",
                       F.col("deviation_category").isin(["SEVERE", "HIGH"])) \
            .withColumn("deviation_reason",
                       F.when(F.col("weather_delay_hours") > 12, "WEATHER")
                        .when(F.col("port_congestion_hours") > 12, "PORT_CONGESTION")
                        .when(F.col("mechanical_issue_flag"), "MECHANICAL")
                        .when(F.col("customs_delay_hours") > 6, "CUSTOMS")
                        .otherwise("OPERATIONAL"))