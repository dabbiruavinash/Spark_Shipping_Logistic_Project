from dataclasses import dataclass
from typing import List, Dict
from enum import Enum

class FileFormat(Enum):
    PARQUET = "parquet"
    DELTA = "delta"
    CSV = "csv"
    JSON = "json"

class DataLayer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

@dataclass
class ADLSConfig:
    storage_account: str
    container: str
    credential_type: str = "managed_identity"
    
@dataclass
class SnowflakeConfig:
    account: str
    user: str
    warehouse: str
    database: str
    schema: str
    role: str

@dataclass
class TableConfig:
    table_name: str
    primary_key: List[str]
    partition_columns: List[str]
    zorder_columns: List[str]
    
@dataclass
class PipelineConfig:
    adls_config: ADLSConfig
    snowflake_config: SnowflakeConfig
    input_paths: Dict[str, str]
    output_paths: Dict[DataLayer, str]
    table_configs: Dict[str, TableConfig]