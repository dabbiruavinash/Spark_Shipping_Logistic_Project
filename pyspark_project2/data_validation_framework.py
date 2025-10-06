from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType
from core.base_processor import BaseProcessor
from core.exceptions import DataValidationError

class DataValidationFramework(BaseProcessor):
    """
    Module: 9.1 - Validates data types, ranges, and referential integrity
    Metadata:
      - Version: 1.3
      - Rules: 50+ validation rules
    """
    
    def validate_schema(self, df: DataFrame, expected_schema: StructType) -> bool:
        """Validate DataFrame schema"""
        
        actual_schema = df.schema
        if actual_schema != expected_schema:
            raise DataValidationError(f"Schema mismatch. Expected: {expected_schema}, Actual: {actual_schema}")
        return True
    
    def validate_data_ranges(self, df: DataFrame, range_rules: dict) -> DataFrame:
        """Validate data value ranges"""
        
        result_df = df
        validation_results = []
        
        for column, (min_val, max_val) in range_rules.items():
            invalid_count = result_df.filter(
                (F.col(column) < min_val) | (F.col(column) > max_val)
            ).count()
            
            validation_results.append({
                "column": column,
                "invalid_count": invalid_count,
                "rule": f"BETWEEN {min_val} AND {max_val}"
            })
        
        return result_df, validation_results
    
    def validate_referential_integrity(self, df: DataFrame, reference_df: DataFrame, key_columns: list) -> DataFrame:
        """Validate referential integrity"""
        
        invalid_records = df.alias("main") \
            .join(reference_df.alias("ref"), key_columns, "left_anti")
        
        return invalid_records