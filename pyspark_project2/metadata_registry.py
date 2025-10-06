"""
Metadata registry for all 50+ PySpark modules in Shipping Logistics Project
"""

from typing import Dict, List
from datetime import datetime
from pyspark.sql import SparkSession

class ModuleMetadata:
    def __init__(self, module_id: str, name: str, version: str, description: str,
                 author: str, dependencies: List[str], input_sources: List[str],
                 output_targets: List[str], business_domain: str, update_frequency: str):
        self.module_id = module_id
        self.name = name
        self.version = version
        self.description = description
        self.author = author
        self.dependencies = dependencies
        self.input_sources = input_sources
        self.output_targets = output_targets
        self.business_domain = business_domain
        self.update_frequency = update_frequency
        self.created_date = datetime.now()
        self.last_updated = datetime.now()

class ModuleMetadataRegistry:
    def __init__(self):
        self.modules: Dict[str, ModuleMetadata] = {}
    
    def register_module(self, metadata: ModuleMetadata):
        self.modules[metadata.module_id] = metadata
    
    def register_all_modules(self, spark: SparkSession):
        """Register all 50+ modules with their metadata"""
        
        # Data Ingestion Layer Modules
        self.register_module(ModuleMetadata(
            module_id="ING-001",
            name="SourceSystemConnector",
            version="1.0",
            description="Connects to multiple source systems (APIs, files, databases)",
            author="Shipping Analytics Team",
            dependencies=["requests", "pyodbc"],
            input_sources=["ERP Systems", "Booking Portals", "AIS Feeds"],
            output_targets=["Bronze Layer"],
            business_domain="Data Integration",
            update_frequency="Real-time"
        ))
        
        # Add metadata for all other 49 modules...
        # [Similar metadata registration for all modules]
        
        # Create metadata table in Delta
        metadata_records = [{
            "module_id": module.module_id,
            "name": module.name,
            "version": module.version,
            "description": module.description,
            "author": module.author,
            "dependencies": ",".join(module.dependencies),
            "input_sources": ",".join(module.input_sources),
            "output_targets": ",".join(module.output_targets),
            "business_domain": module.business_domain,
            "update_frequency": module.update_frequency,
            "created_date": module.created_date,
            "last_updated": module.last_updated
        } for module in self.modules.values()]
        
        metadata_df = spark.createDataFrame(metadata_records)
        metadata_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save("/delta/metadata/module_registry")
    
    def get_module_info(self, module_id: str) -> ModuleMetadata:
        return self.modules.get(module_id)
    
    def get_modules_by_domain(self, business_domain: str) -> List[ModuleMetadata]:
        return [module for module in self.modules.values() 
                if module.business_domain == business_domain]