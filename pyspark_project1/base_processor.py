from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional
from config.pipeline_config import PipelineConfig

class BaseProcessor(ABC):
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def process(self, *args, **kwargs) -> DataFrame:
        pass
    
    def validate_dataframe(self, df: DataFrame, expected_columns: List[str]) -> bool:
        actual_columns = [field.name for field in df.schema.fields]
        return all(col in actual_columns for col in expected_columns)
    
    def cache_dataframe(self, df: DataFrame) -> DataFrame:
        return df.cache()
    
    def persist_dataframe(self, df: DataFrame, storage_level: str = "MEMORY_AND_DISK") -> DataFrame:
        from pyspark import StorageLevel
        storage_level_obj = getattr(StorageLevel, storage_level)
        return df.persist(storage_level_obj)