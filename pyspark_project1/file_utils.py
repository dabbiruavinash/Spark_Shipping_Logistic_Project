import os
from datetime import datetime
from typing import List

class FileUtils:
    @staticmethod
    def get_latest_file_path(directory: str, pattern: str = None) -> str:
        files = [os.path.join(directory, f) for f in os.listdir(directory)]
        if pattern:
            files = [f for f in files if pattern in f]
        return max(files, key=os.path.getctime) if files else None
    
    @staticmethod
    def generate_partition_path(base_path: str, partition_cols: dict) -> str:
        path = base_path
        for col, value in partition_cols.items():
            path = os.path.join(path, f"{col}={value}")
        return path
    
    @staticmethod
    def validate_file_exists(file_path: str) -> bool:
        return os.path.exists(file_path)