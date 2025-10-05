from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F, Window
from typing import List, Dict

class DataTransformationProcessor(BaseProcessor):
    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn("transit_days", F.datediff(F.col("delivery_date"), F.col("shipment_date"))) \
            .withColumn("shipment_month", F.date_format(F.col("shipment_date"), "yyyy-MM")) \
            .withColumn("is_delayed", F.when(
                F.col("delivery_date") > F.date_add(F.col("shipment_date"), 30), True
            ).otherwise(False)) \
            .withColumn("weight_category", F.when(
                F.col("weight_kg") < 10, "Small"
            ).when(F.col("weight_kg") < 100, "Medium").otherwise("Large"))
    
    def handle_missing_values(self, df: DataFrame, strategy: str = "mean") -> DataFrame:
        if strategy == "mean":
            return df.fillna({
                "weight_kg": df.select(F.mean("weight_kg")).first()[0],
                "volume_cbm": df.select(F.mean("volume_cbm")).first()[0]
            })
        elif strategy == "drop":
            return df.dropna(subset=["weight_kg", "volume_cbm"])
        else:
            return df
    
    def normalize_data(self, df: DataFrame, columns: List[str]) -> DataFrame:
        from pyspark.ml.feature import StandardScaler, VectorAssembler
        
        assembler = VectorAssembler(inputCols=columns, outputCol="features")
        assembled_df = assembler.transform(df)
        
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(assembled_df)
        
        return scaler_model.transform(assembled_df).drop("features", "scaled_features")