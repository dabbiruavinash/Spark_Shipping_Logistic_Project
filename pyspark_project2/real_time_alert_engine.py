from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class RealTimeAlertEngine(BaseProcessor):
    """
    Module: 6.3 - Sends alerts for delays or route deviations
    Metadata:
      - Version: 1.2
      - Alert Types: 10+ business alerts
    """
    
    def generate_alerts(self, df: DataFrame) -> DataFrame:
        """Generate real-time business alerts"""
        
        return df \
            .withColumn("alert_type",
                       F.when(F.col("eta_deviation_hours") > 24, "ETA_DEVIATION")
                        .when(F.col("distance_from_route_nm") > 50, "ROUTE_DEVIATION")
                        .when(F.col("fuel_level_percent") < 15, "LOW_FUEL")
                        .when(F.col("weather_risk") == "HIGH", "WEATHER_ALERT")
                        .when(F.col("port_congestion_level") == "HIGH", "PORT_CONGESTION")
                        .otherwise("NO_ALERT")) \
            .withColumn("alert_severity",
                       F.when(F.col("alert_type").isin(["ETA_DEVIATION", "WEATHER_ALERT"]), "HIGH")
                        .when(F.col("alert_type").isin(["ROUTE_DEVIATION", "LOW_FUEL"]), "MEDIUM")
                        .otherwise("LOW")) \
            .withColumn("alert_message",
                       F.concat(F.lit("Alert: "), F.col("alert_type"), 
                               F.lit(" for vessel "), F.col("vessel_id"))) \
            .filter(F.col("alert_type") != "NO_ALERT")