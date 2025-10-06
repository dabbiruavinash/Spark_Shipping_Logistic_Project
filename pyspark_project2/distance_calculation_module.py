from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType
import math
from core.base_processor import BaseProcessor

class DistanceCalculationModule(BaseProcessor):
    """
    Module: 3.1 - Calculates shipping distances using geospatial logic
    Metadata:
      - Version: 1.2
      - Algorithm: Haversine formula
      - Unit: Nautical Miles
    """
    
    @staticmethod
    def haversine_distance(lat1, lon1, lat2, lon2):
        """Calculate great-circle distance using Haversine formula"""
        R = 3440.065  # Earth radius in nautical miles
        
        lat1_rad = F.radians(lat1)
        lon1_rad = F.radians(lon1)
        lat2_rad = F.radians(lat2)
        lon2_rad = F.radians(lon2)
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = F.sin(dlat/2)**2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon/2)**2
        c = 2 * F.atan2(F.sqrt(a), F.sqrt(1-a))
        
        return R * c
    
    def calculate_route_distance(self, df: DataFrame) -> DataFrame:
        """Calculate distance between origin and destination ports"""
        return df.withColumn("distance_nm",
            self.haversine_distance(
                F.col("origin_lat"), F.col("origin_lon"),
                F.col("dest_lat"), F.col("dest_lon")
            ).cast(DoubleType())
        ).withColumn("distance_km", F.col("distance_nm") * 1.852)