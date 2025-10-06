from pyspark.sql import DataFrame, functions as F
import json
from datetime import datetime
from core.base_processor import BaseProcessor

class DataLineageTracker(BaseProcessor):
    """
    Module: 9.2 - Tracks transformations from source to target
    Metadata:
      - Version: 1.1
      - Lineage: Full DAG tracking
    """
    
    def track_transformation(self, input_dfs: list, output_df: DataFrame, 
                           transformation_name: str, metadata: dict):
        """Track data transformation lineage"""
        
        lineage_record = {
            "transformation_id": transformation_name,
            "timestamp": datetime.now().isoformat(),
            "input_tables": [df._jdf.toString() for df in input_dfs],
            "output_table": output_df._jdf.toString(),
            "columns_added": metadata.get("columns_added", []),
            "columns_modified": metadata.get("columns_modified", []),
            "business_rules": metadata.get("business_rules", []),
            "data_quality_checks": metadata.get("data_quality_checks", [])
        }
        
        # Store lineage in Delta table
        lineage_df = self.spark.createDataFrame([lineage_record])
        lineage_df.write \
            .format("delta") \
            .mode("append") \
            .save("/delta/lineage/data_transformations")
    
    def get_lineage_for_table(self, table_name: str) -> DataFrame:
        """Get lineage information for a table"""
        
        return self.spark.read \
            .format("delta") \
            .load("/delta/lineage/data_transformations") \
            .filter(F.col("output_table").contains(table_name))