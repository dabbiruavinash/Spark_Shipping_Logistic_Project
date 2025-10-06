#!/usr/bin/env python3
"""
Shipping Industry PySpark Project - Main Application
50+ Modules Comprehensive Implementation
"""

from config.spark_config import SparkConfig, SparkSessionManager
from config.pipeline_config import PipelineConfig
from config.metadata_registry import ModuleMetadataRegistry

from layers.ingestion.source_system_connector import SourceSystemConnector
from layers.ingestion.shipment_data_ingestion import ShipmentDataIngestion
from layers.ingestion.vessel_position_ingestion import VesselPositionIngestion
from layers.ingestion.port_schedule_ingestion import PortScheduleIngestion
from layers.ingestion.weather_feed_ingestion import WeatherFeedIngestion

from layers.processing.shipment_data_cleaning import ShipmentDataCleaning
from layers.processing.route_data_transformation import RouteDataTransformation
from layers.processing.port_mapping_transformation import PortMappingTransformation
from layers.processing.currency_exchange_transformation import CurrencyExchangeTransformation
from layers.processing.timezone_normalization import TimeZoneNormalization

from layers.enrichment.distance_calculation_module import DistanceCalculationModule
from layers.enrichment.fuel_consumption_estimator import FuelConsumptionEstimator
from layers.enrichment.emission_calculator import EmissionCalculator
from layers.enrichment.weather_impact_analyzer import WeatherImpactAnalyzer
from layers.enrichment.route_optimization_engine import RouteOptimizationEngine

# Import all other modules...
from layers.analytics.operational_kpi_dashboard import OperationalKPIDashboard

import logging
import sys
from datetime import datetime

class ShippingLogisticsOrchestrator:
    """
    Main orchestrator for the Shipping Logistics PySpark Project
    Coordinates 50+ modules in a cohesive pipeline
    """
    
    def __init__(self):
        self.spark_config = SparkConfig(
            app_name="ShippingLogisticsPlatform",
            master="local[*]",
            configs={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skew.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.databricks.delta.retentionDurationCheck.enabled": "false",
                "spark.sql.legacy.timeParserPolicy": "CORRECTED"
            }
        )
        
        self.pipeline_config = PipelineConfig(
            input_paths={
                'shipments': '/data/shipments',
                'vessels': '/data/vessels', 
                'ports': '/data/ports',
                'weather': '/data/weather',
                'exchange_rates': '/data/forex'
            },
            output_paths={
                'bronze': '/delta/bronze',
                'silver': '/delta/silver', 
                'gold': '/delta/gold',
                'reports': '/delta/reports'
            }
        )
        
        self.spark_manager = SparkSessionManager(self.spark_config)
        self.spark = None
        self.metadata_registry = ModuleMetadataRegistry()
    
    def setup_logging(self):
        """Setup comprehensive logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'shipping_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def initialize_spark(self):
        """Initialize Spark session with optimizations"""
        self.logger.info("Initializing Spark Session with optimized configuration...")
        self.spark = self.spark_manager.create_session()
        self.logger.info("Spark Session initialized successfully")
        
        # Register module metadata
        self.metadata_registry.register_all_modules(self.spark)
    
    def run_data_ingestion_layer(self):
        """Execute data ingestion modules"""
        self.logger.info("=== Starting Data Ingestion Layer ===")
        
        # Initialize ingestion modules
        source_connector = SourceSystemConnector(self.spark, self.pipeline_config.__dict__)
        shipment_ingestion = ShipmentDataIngestion(self.spark, self.pipeline_config.__dict__)
        vessel_ingestion = VesselPositionIngestion(self.spark, self.pipeline_config.__dict__)
        port_ingestion = PortScheduleIngestion(self.spark, self.pipeline_config.__dict__)
        weather_ingestion = WeatherFeedIngestion(self.spark, self.pipeline_config.__dict__)
        
        # Execute ingestion
        raw_shipments = shipment_ingestion.ingest_shipment_manifests(
            self.pipeline_config.input_paths['shipments']
        )
        vessel_positions = vessel_ingestion.batch_load_historical_positions(
            self.pipeline_config.input_paths['vessels']
        )
        port_schedules = port_ingestion.ingest_port_schedules(
            self.pipeline_config.input_paths['ports']
        )
        weather_data = weather_ingestion.fetch_weather_data({
            "base_url": "https://api.weather.com/v1",
            "api_key": "your_api_key",
            "locations": [
                {"id": "USLAX", "latitude": 33.75, "longitude": -118.25},
                {"id": "CNSHA", "latitude": 31.23, "longitude": 121.47}
            ]
        })
        
        self.logger.info("Data Ingestion Layer completed successfully")
        return {
            'shipments': raw_shipments,
            'vessel_positions': vessel_positions,
            'port_schedules': port_schedules,
            'weather_data': weather_data
        }
    
    def run_data_processing_layer(self, raw_data):
        """Execute data processing and transformation modules"""
        self.logger.info("=== Starting Data Processing Layer ===")
        
        # Initialize processing modules
        data_cleaning = ShipmentDataCleaning(self.spark, self.pipeline_config.__dict__)
        route_transformation = RouteDataTransformation(self.spark, self.pipeline_config.__dict__)
        port_mapping = PortMappingTransformation(self.spark, self.pipeline_config.__dict__)
        currency_transformation = CurrencyExchangeTransformation(self.spark, self.pipeline_config.__dict__)
        timezone_normalization = TimeZoneNormalization(self.spark, self.pipeline_config.__dict__)
        
        # Execute processing
        cleaned_shipments = data_cleaning.handle_nulls(raw_data['shipments'])
        cleaned_shipments = data_cleaning.remove_duplicates(cleaned_shipments, ['shipment_id'])
        cleaned_shipments = data_cleaning.standardize_text_fields(cleaned_shipments)
        
        transformed_routes = route_transformation.explode_route_waypoints(raw_data['vessel_positions'])
        
        # Load port master data
        port_master = self.spark.read.format("delta").load("/data/master/ports")
        mapped_shipments = port_mapping.map_port_codes(cleaned_shipments, port_master)
        
        # Load exchange rates
        exchange_rates = self.spark.read.format("delta").load(self.pipeline_config.input_paths['exchange_rates'])
        currency_converted = currency_transformation.convert_to_base_currency(mapped_shipments, exchange_rates)
        
        timezone_normalized = timezone_normalization.convert_to_utc(
            currency_converted, ['shipment_date', 'delivery_date']
        )
        
        self.logger.info("Data Processing Layer completed successfully")
        return {
            'processed_shipments': timezone_normalized,
            'transformed_routes': transformed_routes
        }
    
    def run_data_enrichment_layer(self, processed_data):
        """Execute data enrichment modules"""
        self.logger.info("=== Starting Data Enrichment Layer ===")
        
        # Initialize enrichment modules
        distance_calculator = DistanceCalculationModule(self.spark, self.pipeline_config.__dict__)
        fuel_estimator = FuelConsumptionEstimator(self.spark, self.pipeline_config.__dict__)
        emission_calculator = EmissionCalculator(self.spark, self.pipeline_config.__dict__)
        weather_analyzer = WeatherImpactAnalyzer(self.spark, self.pipeline_config.__dict__)
        route_optimizer = RouteOptimizationEngine(self.spark, self.pipeline_config.__dict__)
        
        # Execute enrichment
        shipments_with_distance = distance_calculator.calculate_route_distance(
            processed_data['processed_shipments']
        )
        shipments_with_fuel = fuel_estimator.estimate_fuel_consumption(shipments_with_distance)
        shipments_with_emissions = emission_calculator.calculate_emissions(shipments_with_fuel)
        shipments_with_weather_risk = weather_analyzer.assess_weather_risk(shipments_with_emissions)
        optimized_routes = route_optimizer.optimize_routes(processed_data['transformed_routes'])
        
        self.logger.info("Data Enrichment Layer completed successfully")
        return {
            'enriched_shipments': shipments_with_weather_risk,
            'optimized_routes': optimized_routes
        }
    
    def run_business_logic_layer(self, enriched_data):
        """Execute business logic modules"""
        self.logger.info("=== Starting Business Logic Layer ===")
        
        # Import and initialize business logic modules
        from layers.business_logic.shipment_status_updater import ShipmentStatusUpdater
        from layers.business_logic.eta_deviation_detector import ETADeviationDetector
        from layers.business_logic.demurrage_calculator import DemurrageCalculator
        from layers.business_logic.container_utilization_tracker import ContainerUtilizationTracker
        from layers.business_logic.customs_clearance_processor import CustomsClearanceProcessor
        
        status_updater = ShipmentStatusUpdater(self.spark, self.pipeline_config.__dict__)
        eta_detector = ETADeviationDetector(self.spark, self.pipeline_config.__dict__)
        demurrage_calculator = DemurrageCalculator(self.spark, self.pipeline_config.__dict__)
        utilization_tracker = ContainerUtilizationTracker(self.spark, self.pipeline_config.__dict__)
        customs_processor = CustomsClearanceProcessor(self.spark, self.pipeline_config.__dict__)
        
        # Load additional data
        shipment_events = self.spark.read.format("delta").load("/data/events/shipments")
        customs_events = self.spark.read.format("delta").load("/data/events/customs")
        
        # Execute business logic
        updated_shipments = status_updater.update_shipment_status(
            enriched_data['enriched_shipments'], shipment_events
        )
        shipments_with_deviations = eta_detector.detect_eta_deviations(updated_shipments)
        shipments_with_demurrage = demurrage_calculator.calculate_demurrage_charges(shipments_with_deviations)
        shipments_with_utilization = utilization_tracker.track_container_utilization(shipments_with_demurrage)
        processed_customs = customs_processor.process_customs_events(customs_events)
        
        self.logger.info("Business Logic Layer completed successfully")
        return {
            'business_shipments': shipments_with_utilization,
            'customs_events': processed_customs
        }
    
    def run_storage_layer(self, business_data):
        """Execute storage and Delta Lake management"""
        self.logger.info("=== Starting Storage Layer ===")
        
        from layers.storage.delta_lake_schema_enforcer import DeltaLakeSchemaEnforcer
        from layers.storage.shipment_delta_table_manager import ShipmentDeltaTableManager
        from layers.storage.route_delta_optimizer import RouteDeltaOptimizer
        
        schema_enforcer = DeltaLakeSchemaEnforcer(self.spark, self.pipeline_config.__dict__)
        table_manager = ShipmentDeltaTableManager(self.spark, self.pipeline_config.__dict__)
        route_optimizer = RouteDeltaOptimizer(self.spark, self.pipeline_config.__dict__)
        
        # Write to silver layer
        schema_enforcer.enforce_schema(
            business_data['business_shipments'],
            f"{self.pipeline_config.output_paths['silver']}/shipments",
            expected_schema={}  # Define your schema here
        )
        
        # Optimize tables
        table_manager.optimize_shipment_tables(
            f"{self.pipeline_config.output_paths['silver']}/shipments",
            ['shipment_month', 'origin_port', 'destination_port']
        )
        
        route_optimizer.optimize_route_tables(
            f"{self.pipeline_config.output_paths['silver']}/routes"
        )
        
        self.logger.info("Storage Layer completed successfully")
    
    def run_analytics_layer(self):
        """Execute analytics and reporting modules"""
        self.logger.info("=== Starting Analytics Layer ===")
        
        from layers.analytics.port_performance_dashboard import PortPerformanceDashboard
        from layers.analytics.vessel_utilization_report import VesselUtilizationReport
        from layers.analytics.delay_root_cause_analyzer import DelayRootCauseAnalyzer
        from layers.analytics.emission_analytics_report import EmissionAnalyticsReport
        from layers.analytics.operational_kpi_dashboard import OperationalKPIDashboard
        
        port_dashboard = PortPerformanceDashboard(self.spark, self.pipeline_config.__dict__)
        vessel_report = VesselUtilizationReport(self.spark, self.pipeline_config.__dict__)
        delay_analyzer = DelayRootCauseAnalyzer(self.spark, self.pipeline_config.__dict__)
        emission_report = EmissionAnalyticsReport(self.spark, self.pipeline_config.__dict__)
        kpi_dashboard = OperationalKPIDashboard(self.spark, self.pipeline_config.__dict__)
        
        # Load data for analytics
        silver_shipments = self.spark.read.format("delta").load(
            f"{self.pipeline_config.output_paths['silver']}/shipments"
        )
        silver_routes = self.spark.read.format("delta").load(
            f"{self.pipeline_config.output_paths['silver']}/routes"
        )
        
        # Generate analytics
        port_kpis = port_dashboard.calculate_port_kpis(silver_shipments)
        vessel_utilization = vessel_report.generate_utilization_report(silver_shipments)
        delay_analysis = delay_analyzer.analyze_delay_causes(silver_shipments)
        emission_analytics = emission_report.generate_emission_report(silver_shipments)
        operational_kpis = kpi_dashboard.calculate_operational_kpis(silver_shipments)
        
        # Write analytics to gold layer
        operational_kpis.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{self.pipeline_config.output_paths['gold']}/operational_kpis")
        
        self.logger.info("Analytics Layer completed successfully")
        return {
            'port_kpis': port_kpis,
            'vessel_utilization': vessel_utilization,
            'delay_analysis': delay_analysis,
            'emission_analytics': emission_analytics,
            'operational_kpis': operational_kpis
        }
    
    def run_governance_layer(self):
        """Execute data governance modules"""
        self.logger.info("=== Starting Governance Layer ===")
        
        from layers.governance.data_validation_framework import DataValidationFramework
        from layers.governance.data_quality_score_calculator import DataQualityScoreCalculator
        from layers.governance.audit_log_generator import AuditLogGenerator
        
        validation_framework = DataValidationFramework(self.spark, self.pipeline_config.__dict__)
        quality_scorer = DataQualityScoreCalculator(self.spark, self.pipeline_config.__dict__)
        audit_logger = AuditLogGenerator(self.spark, self.pipeline_config.__dict__)
        
        # Load data for governance
        gold_data = self.spark.read.format("delta").load(
            f"{self.pipeline_config.output_paths['gold']}/operational_kpis"
        )
        
        # Execute governance tasks
        quality_scores = quality_scorer.calculate_quality_score(gold_data)
        
        # Log pipeline execution
        audit_logger.log_pipeline_execution(
            pipeline_name="ShippingLogisticsPlatform",
            status="SUCCESS",
            records_processed=gold_data.count(),
            error_message=None
        )
        
        self.logger.info("Governance Layer completed successfully")
        return quality_scores
    
    def execute_full_pipeline(self):
        """Execute the complete shipping logistics pipeline"""
        try:
            self.setup_logging()
            self.initialize_spark()
            
            self.logger.info("🚢 Starting Shipping Logistics PySpark Pipeline with 50+ Modules 🚢")
            
            # Execute all layers
            raw_data = self.run_data_ingestion_layer()
            processed_data = self.run_data_processing_layer(raw_data)
            enriched_data = self.run_data_enrichment_layer(processed_data)
            business_data = self.run_business_logic_layer(enriched_data)
            self.run_storage_layer(business_data)
            analytics_results = self.run_analytics_layer()
            governance_results = self.run_governance_layer()
            
            self.logger.info("✅ Shipping Logistics Pipeline completed successfully!")
            self.logger.info(f"📊 Generated {len(analytics_results)} analytics datasets")
            self.logger.info(f"🏆 Data Quality Score: {governance_results.get('overall_score', 'N/A')}")
            
            return {
                "status": "SUCCESS",
                "analytics_datasets": len(analytics_results),
                "quality_score": governance_results.get('overall_score', 'N/A'),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline execution failed: {str(e)}")
            
            # Log failure
            if hasattr(self, 'spark') and self.spark:
                from layers.governance.audit_log_generator import AuditLogGenerator
                audit_logger = AuditLogGenerator(self.spark, self.pipeline_config.__dict__)
                audit_logger.log_pipeline_execution(
                    pipeline_name="ShippingLogisticsPlatform",
                    status="FAILED",
                    records_processed=0,
                    error_message=str(e)
                )
            
            raise
        finally:
            if hasattr(self, 'spark') and self.spark:
                self.logger.info("Stopping Spark Session...")
                self.spark_manager.stop_session()

def main():
    """Main entry point for the shipping logistics application"""
    orchestrator = ShippingLogisticsOrchestrator()
    
    # Execute the full pipeline
    result = orchestrator.execute_full_pipeline()
    
    # Print summary
    print("\n" + "="*80)
    print("SHIPPING LOGISTICS PYSPARK PIPELINE - EXECUTION SUMMARY")
    print("="*80)
    print(f"Status: {result['status']}")
    print(f"Analytics Datasets Generated: {result['analytics_datasets']}")
    print(f"Overall Data Quality Score: {result['quality_score']}")
    print(f"Completion Time: {result['timestamp']}")
    print("="*80)

if __name__ == "__main__":
    main()