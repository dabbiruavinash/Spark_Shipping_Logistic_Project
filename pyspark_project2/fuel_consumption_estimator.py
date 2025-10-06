from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType
from core.base_processor import BaseProcessor

class FuelConsumptionEstimator(BaseProcessor):
    """
    Module: 3.2 - Estimates vessel fuel usage based on distance and type
    Metadata:
      - Version: 1.1
      - Factors: Vessel type, speed, distance, weather
    """
    
    def estimate_fuel_consumption(self, df: DataFrame) -> DataFrame:
        """Estimate fuel consumption for voyages"""
        
        # Base consumption rates (tons per nautical mile) by vessel type
        consumption_rates = {
            "CONTAINER": 0.025,
            "TANKER": 0.035,
            "BULK_CARRIER": 0.030,
            "RORO": 0.020,
            "GENERAL_CARGO": 0.015
        }
        
        base_consumption_expr = F.create_map([F.lit(x) for x in 
            sum(consumption_rates.items(), ())])
        
        return df \
            .withColumn("base_fuel_rate", 
                       base_consumption_expr.getItem(F.col("vessel_type"))) \
            .withColumn("speed_factor",
                       F.when(F.col("speed_knots") > 20, 1.3)
                        .when(F.col("speed_knots") > 15, 1.1)
                        .otherwise(1.0)) \
            .withColumn("weather_factor",
                       F.when(F.col("wave_height") > 3.0, 1.2)
                        .when(F.col("wind_speed") > 25, 1.15)
                        .otherwise(1.0)) \
            .withColumn("estimated_fuel_consumption",
                       (F.col("distance_nm") * 
                        F.col("base_fuel_rate") * 
                        F.col("speed_factor") * 
                        F.col("weather_factor")).cast(DoubleType()))