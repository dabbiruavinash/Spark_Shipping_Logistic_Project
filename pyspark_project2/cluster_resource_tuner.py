from pyspark.sql import SparkSession
from core.base_processor import BaseProcessor

class ClusterResourceTuner(BaseProcessor):
    """
    Module: 8.5 - Tunes executors, cores, and memory
    Metadata:
      - Version: 1.2
      - Tuning: Automatic configuration
    """
    
    def optimize_cluster_config(self, data_size_gb: int) -> dict:
        """Optimize cluster configuration based on data size"""
        
        if data_size_gb < 10:
            return {
                "executors": 2,
                "executor_cores": 2,
                "executor_memory": "4g",
                "driver_memory": "2g"
            }
        elif data_size_gb < 100:
            return {
                "executors": 10,
                "executor_cores": 4,
                "executor_memory": "8g",
                "driver_memory": "4g"
            }
        else:
            return {
                "executors": 20,
                "executor_cores": 4,
                "executor_memory": "16g",
                "driver_memory": "8g"
            }
    
    def apply_configuration(self, config: dict):
        """Apply Spark configuration"""
        
        spark_conf = self.spark.sparkContext.getConf()
        
        for key, value in config.items():
            spark_conf.set(key, value)