
from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class CustomsClearanceProcessor(BaseProcessor):
    """
    Module: 4.5 - Tracks and validates customs event data
    Metadata:
      - Version: 1.0
      - Compliance: Customs regulations per country
    """
    
    def process_customs_events(self, df: DataFrame) -> DataFrame:
        """Process customs clearance events"""
        
        return df \
            .withColumn("clearance_duration_hours",
                       (F.unix_timestamp(F.col("clearance_end_time")) - 
                        F.unix_timestamp(F.col("clearance_start_time"))) / 3600) \
            .withColumn("clearance_status",
                       F.when(F.col("is_approved"), "APPROVED")
                        .when(F.col("is_under_inspection"), "UNDER_INSPECTION")
                        .when(F.col("is_document_review"), "DOCUMENT_REVIEW")
                        .otherwise("PENDING")) \
            .withColumn("requires_additional_docs",
                       F.col("missing_documents_count") > 0) \
            .withColumn("clearance_risk_score",
                       (F.col("missing_documents_count") * 0.3 + 
                        F.col("previous_violations_count") * 0.4 + 
                        F.when(F.col("high_risk_commodity"), 0.3).otherwise(0.0)) * 100) \
            .withColumn("clearance_priority",
                       F.when(F.col("clearance_risk_score") > 70, "HIGH")
                        .when(F.col("clearance_risk_score") > 40, "MEDIUM")
                        .otherwise("LOW"))