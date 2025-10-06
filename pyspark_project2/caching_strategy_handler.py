from pyspark.sql import DataFrame
from pyspark import StorageLevel
from core.base_processor import BaseProcessor

class CachingStrategyHandler(BaseProcessor):
    """
    Module: 8.4 - Implements caching and persistence
    Metadata:
      - Version: 1.1
      - Strategies: MEMORY_ONLY, MEMORY_AND_DISK
    """
    
    def apply_caching_strategy(self, df: DataFrame, strategy: str = "MEMORY_AND_DISK") -> DataFrame:
        """Apply caching strategy to DataFrame"""
        
        storage_level = getattr(StorageLevel, strategy)
        return df.persist(storage_level)
    
    def smart_cache(self, df: DataFrame, usage_count_threshold: int = 3) -> DataFrame:
        """Smart caching based on expected usage"""
        
        # Logic to determine if caching is beneficial
        if df.count() < 1000000:  # Less than 1M rows
            return df.cache()
        else:
            return df.persist(StorageLevel.MEMORY_AND_DISK)
    
    def unpersist_dataframes(self, dataframes: list):
        """Unpersist multiple DataFrames"""
        
        for df in dataframes:
            df.unpersist()