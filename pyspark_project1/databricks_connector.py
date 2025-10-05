from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame

class DatabricksConnector(BaseProcessor):
    def create_database(self, database_name: str):
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    
    def register_table(self, df: DataFrame, table_name: str, database: str = "shipping_logistics"):
        df.createOrReplaceTempView("temp_view")
        self.spark.sql(f"""
            CREATE OR REPLACE TABLE {database}.{table_name}
            USING DELTA
            AS SELECT * FROM temp_view
        """)
    
    def optimize_table(self, table_name: str, zorder_columns: List[str]):
        self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({', '.join(zorder_columns)})")
    
    def vacuum_table(self, table_name: str, retention_hours: int = 168):
        self.spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")