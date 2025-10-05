from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame
from connectors.adls_connector import ADLSConnector
from core.data_models import ShippingSchemas
from typing import Dict

class DataIngestionProcessor(BaseProcessor):
    def __init__(self, spark, config):
        super().__init__(spark, config)
        self.adls_connector = ADLSConnector(spark, config)
    
    def ingest_shipment_data(self) -> DataFrame:
        return self.adls_connector.read_from_adls(
            self.config.input_paths['shipments'],
            'parquet',
            ShippingSchemas.SHIPMENT_SCHEMA
        )
    
    def ingest_customer_data(self) -> DataFrame:
        return self.adls_connector.read_from_adls(
            self.config.input_paths['customers'],
            'delta'
        )
    
    def ingest_carrier_data(self) -> DataFrame:
        return self.adls_connector.read_from_adls(
            self.config.input_paths['carriers'],
            'json',
            ShippingSchemas.CARRIER_SCHEMA
        )
    
    def ingest_all_data(self) -> Dict[str, DataFrame]:
        return {
            'shipments': self.ingest_shipment_data(),
            'customers': self.ingest_customer_data(),
            'carriers': self.ingest_carrier_data()
        }