from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType
from core.base_processor import BaseProcessor

class WeatherImpactAnalyzer(BaseProcessor):
    """
    Module: 3.4 - Assesses ETA delay risk due to weather
    Metadata:
      - Version: 1.0
      - Risk Factors: Storm, High Waves, Low Visibility
    """
    
    def assess_weather_risk(self, df: DataFrame) -> DataFrame:
        """Assess weather-related delay risk"""
        
        return df \
            .withColumn("storm_risk",
                F.when(F.col("wind_speed") > 50, "HIGH")
                 .when(F.col("wind_speed") > 35, "MEDIUM")
                 .when(F.col("wind_speed") > 25, "LOW")
                 .otherwise("NONE")) \
            .withColumn("wave_risk",
                F.when(F.col("wave_height") > 6.0, "HIGH")
                 .when(F.col("wave_height") > 4.0, "MEDIUM")
                 .when(F.col("wave_height") > 2.5, "LOW")
                 .otherwise("NONE")) \
            .withColumn("visibility_risk",
                F.when(F.col("visibility") < 1000, "HIGH")
                 .when(F.col("visibility") < 5000, "MEDIUM")
                 .otherwise("NONE")) \
            .withColumn("overall_weather_risk",
                F.when(
                    (F.col("storm_risk") == "HIGH") | 
                    (F.col("wave_risk") == "HIGH") |
                    (F.col("visibility_risk") == "HIGH"), "HIGH")
                 .when(
                    (F.col("storm_risk") == "MEDIUM") | 
                    (F.col("wave_risk") == "MEDIUM") |
                    (F.col("visibility_risk") == "MEDIUM"), "MEDIUM")
                 .otherwise("LOW")) \
            .withColumn("estimated_delay_hours",
                F.when(F.col("overall_weather_risk") == "HIGH", F.rand() * 24 + 12)
                 .when(F.col("overall_weather_risk") == "MEDIUM", F.rand() * 12 + 6)
                 .otherwise(F.rand() * 6))