from pyspark.sql import functions as F
from core.base_processor import BaseProcessor

class RouteDeltaOptimizer(BaseProcessor):
    """
    Module: 5.4 - Optimizes tables with ZORDER on route and vessel ID
    Metadata:
      - Version: 1.0
      - Optimization: ZORDER clustering
    """
    
    def optimize_route_tables(self, table_path: str):
        """Optimize route-related Delta tables"""
        
        # Optimize with ZORDER on frequently filtered columns
        self.spark.sql(f"""
            OPTIMIZE delta.`{table_path}`
            ZORDER BY (route_id, vessel_id, origin_port, destination_port)
        """)
        
        # Collect table statistics
        self.spark.sql(f"""
            ANALYZE TABLE delta.`{table_path}` 
            COMPUTE STATISTICS
        """)