from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F
from core.exceptions import DataValidationError
from typing import List, Dict

class DataValidationProcessor(BaseProcessor):
    def validate_completeness(self, df: DataFrame, critical_columns: List[str]) -> bool:
        for column in critical_columns:
            null_count = df.filter(F.col(column).isNull()).count()
            if null_count > 0:
                raise DataValidationError(f"Column {column} has {null_count} null values")
        return True
    
    def validate_uniqueness(self, df: DataFrame, unique_columns: List[str]) -> bool:
        total_count = df.count()
        distinct_count = df.select(unique_columns).distinct().count()
        if total_count != distinct_count:
            raise DataValidationError(f"Duplicate records found in columns {unique_columns}")
        return True
    
    def validate_data_range(self, df: DataFrame, column_ranges: Dict[str, tuple]) -> bool:
        for column, (min_val, max_val) in column_ranges.items():
            out_of_range_count = df.filter(
                (F.col(column) < min_val) | (F.col(column) > max_val)
            ).count()
            if out_of_range_count > 0:
                raise DataValidationError(f"Column {column} has {out_of_range_count} values out of range")
        return True
    
    def validate_schema(self, df: DataFrame, expected_schema: Dict[str, str]) -> bool:
        actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
        for column, expected_type in expected_schema.items():
            if column not in actual_schema:
                raise DataValidationError(f"Column {column} not found in dataframe")
            if actual_schema[column] != expected_type:
                raise DataValidationError(f"Column {column} type mismatch: expected {expected_type}, got {actual_schema[column]}")
        return True