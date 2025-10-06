from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType
from core.base_processor import BaseProcessor
from core.exceptions import DataValidationError

class ShipmentDataCleaning(BaseProcessor):
    """
    Module: 2.1 - Handles nulls, duplicates, and schema drift
    Metadata:
      - Version: 1.2
      - Handles: Data Quality, Schema Validation
      - Rules: 20+ data quality rules
    """
    
    def handle_nulls(self, df: DataFrame, strategy: str = "advanced") -> DataFrame:
        """Handle null values based on strategy"""
        if strategy == "advanced":
            return df \
                .fillna({
                    "weight_kg": df.select(F.avg("weight_kg")).first()[0],
                    "volume_cbm": 0.0,
                    "customer_id": "UNKNOWN_CUSTOMER",
                    "status": "PENDING"
                }) \
                .dropna(subset=["shipment_id", "vessel_id"])  # Critical columns
        
        elif strategy == "drop":
            return df.dropna()
        else:
            return df.fillna("UNKNOWN")
    
    def remove_duplicates(self, df: DataFrame, key_columns: list) -> DataFrame:
        """Remove duplicate records"""
        window_spec = Window.partitionBy(key_columns).orderBy(F.col("updated_at").desc())
        
        return df.withColumn("row_num", F.row_number().over(window_spec)) \
                 .filter(F.col("row_num") == 1) \
                 .drop("row_num")
    
    def handle_schema_drift(self, df: DataFrame, expected_schema: dict) -> DataFrame:
        """Handle schema evolution and drift"""
        current_columns = set(df.columns)
        expected_columns = set(expected_schema.keys())
        
        # Add missing columns
        for col in expected_columns - current_columns:
            df = df.withColumn(col, F.lit(None).cast(expected_schema[col]))
        
        # Remove extra columns
        columns_to_keep = [col for col in df.columns if col in expected_columns]
        return df.select(*columns_to_keep)
    
    def standardize_text_fields(self, df: DataFrame) -> DataFrame:
        """Standardize text fields (uppercase, trim)"""
        text_columns = [field.name for field in df.schema.fields 
                       if isinstance(field.dataType, StringType)]
        
        for col in text_columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))
        
        return df