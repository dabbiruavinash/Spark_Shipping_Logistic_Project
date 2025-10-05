from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class SparkConfig:
    app_name: str = "ShippingLogisticsPipeline"
    master: str = "local[*]"
    log_level: str = "WARN"
    
    # Spark configurations
    configs: Dict[str, str] = None
    
    def __post_init__(self):
        if self.configs is None:
            self.configs = {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skew.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.databricks.delta.retentionDurationCheck.enabled": "false"
            }

class SparkSessionManager:
    def __init__(self, config: SparkConfig):
        self.config = config
        self.spark = None
    
    def create_session(self) -> SparkSession:
        spark_builder = SparkSession.builder.appName(self.config.app_name)
        
        for key, value in self.config.configs.items():
            spark_builder.config(key, value)
        
        self.spark = spark_builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(self.config.log_level)
        return self.spark
    
    def stop_session(self):
        if self.spark:
            self.spark.stop()