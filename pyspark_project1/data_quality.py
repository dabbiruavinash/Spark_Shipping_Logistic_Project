from core.base_processor import BaseProcessor
from pyspark.sql import DataFrame, functions as F
from datetime import datetime

class DataQualityProcessor(BaseProcessor):
    def run_data_quality_checks(self, df: DataFrame, table_name: str) -> Dict:
        checks = {}
        
        # Record count check
        checks['record_count'] = df.count()
        
        # Null checks
        null_checks = {}
        for column in df.columns:
            null_count = df.filter(F.col(column).isNull()).count()
            null_checks[column] = null_count
        
        checks['null_counts'] = null_checks
        
        # Duplicate check
        duplicate_count = df.count() - df.distinct().count()
        checks['duplicate_count'] = duplicate_count
        
        # Data freshness check
        if 'updated_at' in df.columns:
            max_updated = df.select(F.max("updated_at")).first()[0]
            checks['data_freshness_days'] = (datetime.now() - max_updated).days if max_updated else None
        
        self._log_data_quality_results(table_name, checks)
        return checks
    
    def _log_data_quality_results(self, table_name: str, checks: Dict):
        self.logger.info(f"Data Quality Results for {table_name}:")
        for check, value in checks.items():
            self.logger.info(f"  {check}: {value}")