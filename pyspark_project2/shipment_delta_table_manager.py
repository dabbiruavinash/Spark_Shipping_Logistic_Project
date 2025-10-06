from delta.tables import DeltaTable
from pyspark.sql import functions as F
from core.base_processor import BaseProcessor

class ShipmentDeltaTableManager(BaseProcessor):
    """
    Module: 5.3 - Manages ACID-compliant shipment tables
    Metadata:
      - Version: 1.2
      - ACID: Full transactional support
    """
    
    def optimize_shipment_tables(self, table_path: str, zorder_columns: list):
        """Optimize Delta tables for shipment queries"""
        
        self.spark.sql(f"""
            OPTIMIZE delta.`{table_path}`
            ZORDER BY ({', '.join(zorder_columns)})
        """)
    
    def vacuum_old_files(self, table_path: str, retention_hours: int = 168):
        """Vacuum old files to save storage"""
        
        self.spark.sql(f"""
            VACUUM delta.`{table_path}` 
            RETAIN {retention_hours} HOURS
        """)
    
    def time_travel_query(self, table_path: str, version: int = None, timestamp: str = None):
        """Query historical versions of the table"""
        
        reader = self.spark.read.format("delta")
        
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)
        
        return reader.load(table_path)