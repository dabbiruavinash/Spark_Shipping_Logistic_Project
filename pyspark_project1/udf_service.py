from core.base_processor import BaseProcessor
from pyspark.sql.types import StringType, DoubleType, ArrayType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

class UDFService(BaseProcessor):
    def __init__(self, spark, config):
        super().__init__(spark, config)
        self._register_udfs()
    
    def _register_udfs(self):
        # Regular UDFs
        self.spark.udf.register("get_region_from_port", self.get_region_from_port, StringType())
        self.spark.udf.register("calculate_shipping_cost", self.calculate_shipping_cost, DoubleType())
        
        # Pandas UDF
        self.calculate_complex_metrics_udf = pandas_udf(
            self.calculate_complex_metrics, 
            ArrayType(DoubleType()),
            PandasUDFType.GROUPED_AGG
        )
    
    @staticmethod
    def get_region_from_port(port_code: str) -> str:
        regions = {
            'US': 'North America',
            'CN': 'Asia',
            'DE': 'Europe',
            'JP': 'Asia',
            'SG': 'Asia',
            'NL': 'Europe',
            'GB': 'Europe'
        }
        if port_code and len(port_code) >= 2:
            country_code = port_code[:2]
            return regions.get(country_code, 'Other')
        return 'Unknown'
    
    @staticmethod
    def calculate_shipping_cost(weight: float, volume: float, distance: float) -> float:
        base_cost = 10.0
        weight_cost = weight * 0.5
        volume_cost = volume * 20.0
        distance_cost = distance * 0.01
        return base_cost + weight_cost + volume_cost + distance_cost
    
    @staticmethod
    def calculate_complex_metrics(weight: pd.Series, volume: pd.Series) -> pd.Series:
        density = weight / volume
        cost_efficiency = weight / (volume * 0.5)
        return pd.Series([[density.mean(), cost_efficiency.mean()]])