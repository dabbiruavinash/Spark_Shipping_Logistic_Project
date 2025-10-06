from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class PortScheduleIngestion(BaseProcessor):
    """
    Module: 1.4 - Loads port schedules and docking information
    Metadata:
      - Version: 1.0
      - Sources: Port Authorities, Terminal Operators
      - Update Frequency: Daily
    """
    
    def ingest_port_schedules(self, schedule_path: str) -> DataFrame:
        """Ingest port schedule data"""
        schedules = self.spark.read.format("json").load(schedule_path)
        
        return schedules \
            .withColumn("schedule_date", F.to_date(F.col("planned_time"))) \
            .withColumn("berth_window_start", F.col("planned_time")) \
            .withColumn("berth_window_end", 
                       F.col("planned_time") + F.expr("INTERVAL 2 HOURS")) \
            .withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("is_active", F.lit(True))