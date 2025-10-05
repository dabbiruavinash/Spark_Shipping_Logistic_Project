from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame
from core.exceptions import ConnectionError

class SnowflakeConnector(BaseProcessor):
    def __init__(self, spark, config):
        super().__init__(spark, config)
        self.sf_options = self._build_snowflake_options()
    
    def _build_snowflake_options(self):
        sf_config = self.config.snowflake_config
        return {
            "sfUrl": f"{sf_config.account}.snowflakecomputing.com",
            "sfUser": sf_config.user,
            "sfWarehouse": sf_config.warehouse,
            "sfDatabase": sf_config.database,
            "sfSchema": sf_config.schema,
            "sfRole": sf_config.role
        }
    
    def write_to_snowflake(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        try:
            (df.write
                .format("snowflake")
                .options(**self.sf_options)
                .option("dbtable", table_name)
                .mode(mode)
                .save())
        except Exception as e:
            raise ConnectionError(f"Failed to write to Snowflake: {str(e)}")
    
    def read_from_snowflake(self, table_name: str) -> DataFrame:
        try:
            return (self.spark.read
                    .format("snowflake")
                    .options(**self.sf_options)
                    .option("dbtable", table_name)
                    .load())
        except Exception as e:
            raise ConnectionError(f"Failed to read from Snowflake: {str(e)}")
    
    def execute_snowflake_query(self, query: str):
        try:
            # Using JDBC for complex queries
            df = self.spark.read \
                .format("snowflake") \
                .options(**self.sf_options) \
                .option("query", query) \
                .load()
            return df
        except Exception as e:
            raise ConnectionError(f"Failed to execute Snowflake query: {str(e)}")