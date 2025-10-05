from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import json

class SparkUtils:
    @staticmethod
    def show_execution_plan(df: DataFrame):
        df.explain(extended=True)
    
    @staticmethod
    def get_dataframe_size(df: DataFrame) -> int:
        return df.rdd.count()
    
    @staticmethod
    def get_partition_info(df: DataFrame) -> dict:
        return {
            'num_partitions': df.rdd.getNumPartitions(),
            'partition_sizes': [len(part) for part in df.rdd.glom().collect()]
        }
    
    @staticmethod
    def optimize_dataframe(df: DataFrame, partition_cols: list = None) -> DataFrame:
        if partition_cols:
            return df.repartition(*partition_cols)
        else:
            return df.coalesce(200)  # Adjust based on your cluster size
    
    @staticmethod
    def cache_if_large(df: DataFrame, threshold: int = 1000000) -> DataFrame:
        if df.count() > threshold:
            return df.cache()
        return df