from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType
from core.base_processor import BaseProcessor

class ProfitMarginAnalyzer(BaseProcessor):
    """
    Module: 7.3 - Computes profit per voyage or route
    Metadata:
      - Version: 1.1
      - Margin Analysis: Gross, Net, Operating
    """
    
    def analyze_profit_margins(self, df: DataFrame) -> DataFrame:
        """Analyze profit margins for voyages"""
        
        return df \
            .withColumn("total_revenue", 
                       F.col("freight_charges") + F.col("additional_services_revenue")) \
            .withColumn("total_costs",
                       F.col("fuel_costs") + F.col("port_charges") + 
                       F.col("crew_costs") + F.col("maintenance_costs") + 
                       F.col("insurance_costs")) \
            .withColumn("gross_profit", 
                       F.col("total_revenue") - F.col("total_costs")) \
            .withColumn("gross_margin_percent",
                       (F.col("gross_profit") / F.col("total_revenue")) * 100) \
            .withColumn("profitability_category",
                       F.when(F.col("gross_margin_percent") > 25, "HIGH")
                        .when(F.col("gross_margin_percent") > 15, "MEDIUM")
                        .when(F.col("gross_margin_percent") > 5, "LOW")
                        .otherwise("LOSS")) \
            .withColumn("contribution_margin",
                       (F.col("gross_profit") / F.col("distance_nm"))
                       .cast(DecimalType(10, 2)))