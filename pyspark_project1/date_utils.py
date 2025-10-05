from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

class DateUtils:
    @staticmethod
    def get_date_range(start_date: str, end_date: str) -> list:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        return [(start + timedelta(days=x)).strftime('%Y-%m-%d') 
                for x in range(0, (end - start).days + 1)]
    
    @staticmethod
    def add_business_days(df, date_col, days_col, new_col_name):
        # Simplified business days calculation
        return df.withColumn(new_col_name, 
            F.date_add(F.col(date_col), 
                      F.col(days_col) + (F.col(days_col) / 5 * 2)))