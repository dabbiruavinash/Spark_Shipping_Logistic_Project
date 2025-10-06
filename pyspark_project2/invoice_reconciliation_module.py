from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class InvoiceReconciliationModule(BaseProcessor):
    """
    Module: 7.4 - Matches shipment and invoice data
    Metadata:
      - Version: 1.0
      - Matching: Fuzzy + Exact matching
    """
    
    def reconcile_invoices(self, shipments_df: DataFrame, invoices_df: DataFrame) -> DataFrame:
        """Reconcile shipment data with invoices"""
        
        return shipments_df.alias("s") \
            .join(invoices_df.alias("i"), 
                  (F.col("s.shipment_id") == F.col("i.shipment_id")) &
                  (F.abs(F.col("s.total_charges") - F.col("i.invoice_amount")) < 0.01),
                  "left_outer") \
            .withColumn("reconciliation_status",
                       F.when(F.col("i.invoice_id").isNull(), "UNINVOICED")
                        .when(F.col("s.total_charges") == F.col("i.invoice_amount"), "MATCHED")
                        .when(F.col("s.total_charges") > F.col("i.invoice_amount"), "UNDER_CHARGED")
                        .otherwise("OVER_CHARGED")) \
            .withColumn("discrepancy_amount",
                       F.coalesce(F.col("s.total_charges") - F.col("i.invoice_amount"), 
                                 F.col("s.total_charges")))