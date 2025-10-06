from pyspark.sql import DataFrame, SparkSession
from typing import Dict, List, Optional
import requests
import json
from core.base_processor import BaseProcessor
from core.exceptions import DataIngestionError

class SourceSystemConnector(BaseProcessor):
    """
    Module: 1.1 - Connects to multiple source systems (APIs, files, databases)
    Metadata: 
      - Version: 1.0
      - Author: Shipping Analytics Team
      - Description: Unified connector for multiple data sources
      - Dependencies: requests, pyodbc, jaydebeapi
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        super().__init__(spark, config)
        self.api_timeout = config.get('api_timeout', 30)
    
    def read_from_api(self, endpoint: str, headers: Dict = None) -> DataFrame:
        """Read data from REST API"""
        try:
            response = requests.get(endpoint, headers=headers, timeout=self.api_timeout)
            response.raise_for_status()
            
            data = response.json()
            # Convert JSON to RDD then to DataFrame
            rdd = self.spark.sparkContext.parallelize([json.dumps(record) for record in data])
            return self.spark.read.json(rdd)
            
        except Exception as e:
            raise DataIngestionError(f"API ingestion failed for {endpoint}: {str(e)}")
    
    def read_from_jdbc(self, url: str, table: str, properties: Dict) -> DataFrame:
        """Read data from JDBC source"""
        return self.spark.read.jdbc(url=url, table=table, properties=properties)
    
    def read_multiple_files(self, file_paths: List[str], file_format: str) -> DataFrame:
        """Read multiple files of same format"""
        return self.spark.read.format(file_format).load(file_paths)
    
    def stream_from_kafka(self, topics: List[str], kafka_config: Dict) -> DataFrame:
        """Stream data from Kafka topics"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
            .option("subscribe", ",".join(topics)) \
            .options(**kafka_config.get("options", {})) \
            .load()