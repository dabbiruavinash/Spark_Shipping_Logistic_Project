from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from core.base_processor import BaseProcessor

class DelayRootCauseAnalyzer(BaseProcessor):
    """
    Module: 10.3 - Identifies causes of late shipments
    Metadata:
      - Version: 1.2
      - Analysis: Correlation, Clustering, Root Cause
    """
    
    def analyze_delay_causes(self, df: DataFrame) -> DataFrame:
        """Analyze root causes of shipment delays"""
        
        # Prepare features for clustering
        feature_columns = [
            "weather_delay_hours", "port_congestion_hours", 
            "mechanical_delay_hours", "customs_delay_hours",
            "labor_delay_hours"
        ]
        
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        assembled_df = assembler.transform(df)
        
        # Cluster delays to find patterns
        kmeans = KMeans(k=4, seed=42, featuresCol="features")
        model = kmeans.fit(assembled_df)
        
        clustered_df = model.transform(assembled_df)
        
        return clustered_df \
            .withColumn("primary_delay_cause",
                       F.when(F.col("prediction") == 0, "WEATHER")
                        .when(F.col("prediction") == 1, "PORT_CONGESTION")
                        .when(F.col("prediction") == 2, "MECHANICAL")
                        .when(F.col("prediction") == 3, "CUSTOMS_LABOR")
                        .otherwise("OTHER")) \
            .withColumn("delay_severity",
                       F.when(F.col("total_delay_hours") > 72, "SEVERE")
                        .when(F.col("total_delay_hours") > 48, "HIGH")
                        .when(F.col("total_delay_hours") > 24, "MEDIUM")
                        .otherwise("MINOR"))