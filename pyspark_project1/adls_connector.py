from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame
from typing import List
from core.exceptions import ConnectionError

class ADLSConnector(BaseProcessor):
    def read_from_adls(self, file_path: str, file_format: str, schema=None) -> DataFrame:
        try:
            full_path = f"abfss://{self.config.adls_config.container}@{self.config.adls_config.storage_account}.dfs.core.windows.net/{file_path}"
            
            reader = self.spark.read
            if schema:
                reader = reader.schema(schema)
            
            return reader.format(file_format).load(full_path)
        except Exception as e:
            raise ConnectionError(f"Failed to read from ADLS: {str(e)}")
    
    def write_to_adls(self, df: DataFrame, file_path: str, file_format: str, mode: str = "overwrite", partition_by: List[str] = None):
        try:
            full_path = f"abfss://{self.config.adls_config.container}@{self.config.adls_config.storage_account}.dfs.core.windows.net/{file_path}"
            
            writer = df.write.format(file_format).mode(mode)
            if partition_by:
                writer = writer.partitionBy(partition_by)
            
            writer.save(full_path)
        except Exception as e:
            raise ConnectionError(f"Failed to write to ADLS: {str(e)}")
    
    def read_delta_table(self, table_path: str, version: int = None) -> DataFrame:
        try:
            full_path = f"abfss://{self.config.adls_config.container}@{self.config.adls_config.storage_account}.dfs.core.windows.net/{table_path}"
            
            reader = self.spark.read.format("delta")
            if version is not None:
                reader = reader.option("versionAsOf", version)
            
            return reader.load(full_path)
        except Exception as e:
            raise ConnectionError(f"Failed to read Delta table: {str(e)}")