from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class EmissionCalculator(BaseProcessor):
    """
    Module: 3.3 - Calculates CO₂ and NOx emissions per voyage
    Metadata:
      - Version: 1.1
      - Standards: IMO GHG Guidelines
      - Compliance: MARPOL Annex VI
    """
    
    def calculate_emissions(self, df: DataFrame) -> DataFrame:
        """Calculate CO2 and NOx emissions"""
        
        # Emission factors (kg per ton of fuel)
        emission_factors = {
            "CO2": 3200,  # kg CO2 per ton of fuel
            "NOx": 87,    # kg NOx per ton of fuel
            "SOx": 54     # kg SOx per ton of fuel
        }
        
        return df \
            .withColumn("co2_emissions_kg",
                       F.col("estimated_fuel_consumption") * emission_factors["CO2"]) \
            .withColumn("nox_emissions_kg", 
                       F.col("estimated_fuel_consumption") * emission_factors["NOx"]) \
            .withColumn("sox_emissions_kg",
                       F.col("estimated_fuel_consumption") * emission_factors["SOx"]) \
            .withColumn("co2_per_teu",
                       F.col("co2_emissions_kg") / F.col("container_count")) \
            .withColumn("emission_intensity",
                       F.col("co2_emissions_kg") / F.col("distance_nm"))