from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F

class AggregationService(BaseProcessor):
    def multi_level_aggregation(self, df: DataFrame, dimensions: list, metrics: dict) -> DataFrame:
        return df.groupBy(dimensions).agg(*[
            F.expr(f"{agg_func}({col}) as {alias}")
            for col, (agg_func, alias) in metrics.items()
        ])
    
    def rollup_aggregation(self, df: DataFrame, dimensions: list, metrics: dict) -> DataFrame:
        return df.rollup(*dimensions).agg(*[
            F.expr(f"{agg_func}({col}) as {alias}")
            for col, (agg_func, alias) in metrics.items()
        ])
    
    def cube_aggregation(self, df: DataFrame, dimensions: list, metrics: dict) -> DataFrame:
        return df.cube(*dimensions).agg(*[
            F.expr(f"{agg_func}({col}) as {alias}")
            for col, (agg_func, alias) in metrics.items()
        ])
    
    def pivot_aggregation(self, df: DataFrame, pivot_col: str, values: list, 
                         dimensions: list, metric_col: str) -> DataFrame:
        return df.groupBy(dimensions).pivot(pivot_col, values).agg(
            F.sum(metric_col).alias("sum"),
            F.avg(metric_col).alias("avg")
        )