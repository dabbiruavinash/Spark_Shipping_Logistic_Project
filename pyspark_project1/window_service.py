from core.base_processor import BaseProcessor
from pyspark.sql import Window, functions as F
from pyspark.sql.types import *

class WindowService(BaseProcessor):
    def create_rolling_window(self, partition_cols: list, order_col: str, window_days: int):
        return Window.partitionBy(partition_cols) \
            .orderBy(F.col(order_col).cast("timestamp").cast("long")) \
            .rangeBetween(-window_days * 86400, 0)
    
    def calculate_moving_averages(self, df: DataFrame, value_col: str, partition_cols: list, 
                                order_col: str, windows: list) -> DataFrame:
        result_df = df
        for window in windows:
            window_spec = self.create_rolling_window(partition_cols, order_col, window)
            result_df = result_df.withColumn(
                f"moving_avg_{window}d_{value_col}",
                F.avg(value_col).over(window_spec)
            )
        return result_df
    
    def calculate_cumulative_sums(self, df: DataFrame, value_col: str, partition_cols: list, 
                                order_col: str) -> DataFrame:
        window_spec = Window.partitionBy(partition_cols).orderBy(order_col).rowsBetween(Window.unboundedPreceding, 0)
        return df.withColumn(f"cumulative_{value_col}", F.sum(value_col).over(window_spec))