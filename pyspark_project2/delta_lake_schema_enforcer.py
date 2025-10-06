
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable
from core.base_processor import BaseProcessor

class DeltaLakeSchemaEnforcer(BaseProcessor):
    """
    Module: 5.1 - Ensures schema consistency using Delta Lake
    Metadata:
      - Version: 1.2
      - Features: Schema validation, Data quality
    """
    
    def enforce_schema(self, df: DataFrame, table_path: str, expected_schema: dict):
        """Enforce schema on Delta table"""
        
        # Check if table exists
        if DeltaTable.isDeltaTable(self.spark, table_path):
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Merge data with schema enforcement
            delta_table.alias("target") \
                .merge(df.alias("source"), "target.shipment_id = source.shipment_id") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            # Create new table with expected schema
            df.write \
                .format("delta") \
                .option("mergeSchema", "true") \
                .save(table_path)
    
    def add_data_quality_checks(self, table_path: str, constraints: dict):
        """Add data quality constraints to Delta table"""
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Add check constraints
        for constraint_name, constraint_expr in constraints.items():
            self.spark.sql(f"""
                ALTER TABLE delta.`{table_path}` 
                ADD CONSTRAINT {constraint_name} 
                CHECK ({constraint_expr})
            """)