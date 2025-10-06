from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from core.base_processor import BaseProcessor

class DeltaLakeSchemaEvolution(BaseProcessor):
    """
    Module: 5.2 - Handles evolving source schema automatically
    Metadata:
      - Version: 1.1
      - Feature: Automatic schema evolution
    """
    
    def handle_schema_evolution(self, df: DataFrame, table_path: str):
        """Handle schema evolution automatically"""
        
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .option("autoMigrate", "true") \
            .save(table_path)
    
    def backfill_new_columns(self, table_path: str, backfill_rules: dict):
        """Backfill data for newly added columns"""
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        for column, default_value in backfill_rules.items():
            self.spark.sql(f"""
                UPDATE delta.`{table_path}` 
                SET {column} = {default_value} 
                WHERE {column} IS NULL
            """)