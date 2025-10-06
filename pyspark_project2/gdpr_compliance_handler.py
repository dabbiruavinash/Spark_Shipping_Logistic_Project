from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable
from core.base_processor import BaseProcessor

class GDPRComplianceHandler(BaseProcessor):
    """
    Module: 9.4 - Handles deletion requests and PII masking
    Metadata:
      - Version: 1.1
      - Compliance: GDPR, CCPA
    """
    
    def mask_pii_data(self, df: DataFrame, pii_columns: list) -> DataFrame:
        """Mask PII data for compliance"""
        
        result_df = df
        for column in pii_columns:
            result_df = result_df.withColumn(
                column,
                F.when(F.col(column).isNotNull(), 
                       F.sha2(F.col(column), 256))
                 .otherwise(F.col(column))
            )
        return result_df
    
    def handle_deletion_request(self, table_path: str, customer_id: str):
        """Handle GDPR deletion requests"""
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Soft delete by masking PII
        delta_table.update(
            condition = F.col("customer_id") == customer_id,
            set = {
                "customer_name": F.lit("DELETED"),
                "customer_email": F.lit("DELETED"),
                "customer_phone": F.lit("DELETED"),
                "is_deleted": F.lit(True)
            }
        )
    
    def anonymize_for_analytics(self, df: DataFrame) -> DataFrame:
        """Anonymize data for analytics use"""
        
        return df \
            .withColumn("customer_id", F.sha2(F.col("customer_id"), 256)) \
            .drop("customer_name", "customer_email", "customer_phone", "customer_address")