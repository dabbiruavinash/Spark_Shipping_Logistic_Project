from pyspark.sql import DataFrame, functions as F
from datetime import datetime
from core.base_processor import BaseProcessor

class AuditLogGenerator(BaseProcessor):
    """
    Module: 9.5 - Generates audit trail for pipeline executions
    Metadata:
      - Version: 1.0
      - Audit: Full pipeline execution tracking
    """
    
    def log_pipeline_execution(self, pipeline_name: str, status: str, 
                             records_processed: int, error_message: str = None):
        """Log pipeline execution details"""
        
        audit_record = {
            "pipeline_name": pipeline_name,
            "execution_timestamp": datetime.now(),
            "status": status,
            "records_processed": records_processed,
            "error_message": error_message,
            "executor_user": self.spark.sparkContext.sparkUser(),
            "application_id": self.spark.sparkContext.applicationId
        }
        
        audit_df = self.spark.createDataFrame([audit_record])
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .save("/delta/audit/pipeline_executions")
    
    def get_execution_stats(self, pipeline_name: str, days: int = 30) -> DataFrame:
        """Get execution statistics for a pipeline"""
        
        return self.spark.read \
            .format("delta") \
            .load("/delta/audit/pipeline_executions") \
            .filter(
                (F.col("pipeline_name") == pipeline_name) &
                (F.col("execution_timestamp") >= F.current_date() - days)
            ) \
            .groupBy("status") \
            .agg(
                F.count("*").alias("execution_count"),
                F.avg("records_processed").alias("avg_records_processed"),
                F.min("execution_timestamp").alias("first_execution"),
                F.max("execution_timestamp").alias("last_execution")
            )