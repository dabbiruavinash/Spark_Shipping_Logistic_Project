from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class EmissionAnalyticsReport(BaseProcessor):
    """
    Module: 10.4 - Provides emission statistics per voyage
    Metadata:
      - Version: 1.1
      - Standards: IMO DCS, EU MRV
    """
    
    def generate_emission_report(self, df: DataFrame) -> DataFrame:
        """Generate comprehensive emission analytics report"""
        
        return df.groupBy("vessel_id", "vessel_type", "year", "quarter") \
            .agg(
                F.sum("co2_emissions_kg").alias("total_co2_emissions"),
                F.sum("nox_emissions_kg").alias("total_nox_emissions"),
                F.sum("sox_emissions_kg").alias("total_sox_emissions"),
                F.sum("distance_nm").alias("total_distance"),
                F.sum("fuel_consumption").alias("total_fuel_consumption"),
                F.sum("cargo_tonnes").alias("total_cargo")
            ) \
            .withColumn("co2_efficiency", 
                       F.col("total_co2_emissions") / F.col("total_distance")) \
            .withColumn("carbon_intensity",
                       F.col("total_co2_emissions") / (F.col("total_cargo") * F.col("total_distance"))) \
            .withColumn("compliance_status",
                       F.when(F.col("carbon_intensity") < 10, "COMPLIANT")
                        .when(F.col("carbon_intensity") < 15, "WATCH")
                        .otherwise("NON_COMPLIANT")) \
            .withColumn("year_over_year_trend",
                       (F.col("carbon_intensity") - F.lag("carbon_intensity").over(
                           Window.partitionBy("vessel_id").orderBy("year", "quarter")
                       )) / F.lag("carbon_intensity").over(
                           Window.partitionBy("vessel_id").orderBy("year", "quarter")
                       ) * 100)