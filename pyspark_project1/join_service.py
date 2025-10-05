from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import broadcast

class JoinService(BaseProcessor):
    def perform_inner_join(self, left_df: DataFrame, right_df: DataFrame, join_cols: list) -> DataFrame:
        return left_df.join(right_df, join_cols, "inner")
    
    def perform_left_join(self, left_df: DataFrame, right_df: DataFrame, join_cols: list) -> DataFrame:
        return left_df.join(right_df, join_cols, "left")
    
    def perform_broadcast_join(self, left_df: DataFrame, right_df: DataFrame, join_cols: list) -> DataFrame:
        return left_df.join(broadcast(right_df), join_cols, "inner")
    
    def handle_skew_join(self, left_df: DataFrame, right_df: DataFrame, join_col: str, 
                        skew_threshold: int = 100000) -> DataFrame:
        # Check for skew
        left_counts = left_df.groupBy(join_col).count()
        skewed_keys = left_counts.filter(F.col("count") > skew_threshold).select(join_col)
        
        if skewed_keys.count() > 0:
            # Add salt to handle skew
            left_salted = left_df.crossJoin(
                self.spark.range(0, 10).withColumnRenamed("id", "salt")
            ).withColumn("salted_join_key", 
                       F.concat(F.col(join_col), F.lit("_"), F.col("salt")))
            
            right_salted = right_df.crossJoin(
                self.spark.range(0, 10).withColumnRenamed("id", "salt")
            ).withColumn("salted_join_key", 
                       F.concat(F.col(join_col), F.lit("_"), F.col("salt")))
            
            return left_salted.join(right_salted, "salted_join_key") \
                .drop("salt", "salted_join_key")
        else:
            return left_df.join(right_df, join_col)