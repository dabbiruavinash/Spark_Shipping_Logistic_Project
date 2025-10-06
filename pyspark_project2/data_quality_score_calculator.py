from pyspark.sql import DataFrame, functions as F
from core.base_processor import BaseProcessor

class DataQualityScoreCalculator(BaseProcessor):
    """
    Module: 9.3 - Assigns quality scores to datasets
    Metadata:
      - Version: 1.2
      - Dimensions: Completeness, Accuracy, Consistency, Timeliness
    """
    
    def calculate_quality_score(self, df: DataFrame) -> dict:
        """Calculate comprehensive data quality score"""
        
        total_records = df.count()
        
        # Completeness Score
        completeness_scores = []
        for column in df.columns:
            non_null_count = df.filter(F.col(column).isNotNull()).count()
            completeness = (non_null_count / total_records) * 100
            completeness_scores.append(completeness)
        
        avg_completeness = sum(completeness_scores) / len(completeness_scores)
        
        # Accuracy Score (based on validation rules)
        accuracy_checks = [
            df.filter(F.col("shipment_id").rlike("^SHIP\\d+$")).count() / total_records,
            df.filter(F.col("weight_kg") > 0).count() / total_records,
            df.filter(F.col("volume_cbm") > 0).count() / total_records
        ]
        avg_accuracy = sum(accuracy_checks) / len(accuracy_checks) * 100
        
        # Consistency Score
        consistency_checks = [
            df.filter(F.col("arrival_date") >= F.col("departure_date")).count() / total_records,
            df.filter(F.length(F.col("port_code")) == 5).count() / total_records
        ]
        avg_consistency = sum(consistency_checks) / len(consistency_checks) * 100
        
        # Overall Quality Score
        overall_score = (avg_completeness * 0.3 + avg_accuracy * 0.4 + avg_consistency * 0.3)
        
        return {
            "overall_score": overall_score,
            "completeness_score": avg_completeness,
            "accuracy_score": avg_accuracy,
            "consistency_score": avg_consistency,
            "total_records": total_records,
            "assessment_date": datetime.now().isoformat()
        }