from config.spark_config import SparkConfig, SparkSessionManager
from config.pipeline_config import PipelineConfig, ADLSConfig, SnowflakeConfig, DataLayer, TableConfig
from processors.data_ingestion import DataIngestionProcessor
from processors.data_validation import DataValidationProcessor
from processors.data_transformation import DataTransformationProcessor
from processors.data_enrichment import DataEnrichmentProcessor
from processors.data_quality import DataQualityProcessor
from transformations.shipment_transformer import ShipmentTransformer
from transformations.customer_transformer import CustomerTransformer
from transformations.route_transformer import RouteTransformer
from transformations.financial_transformer import FinancialTransformer
from connectors.adls_connector import ADLSConnector
from connectors.snowflake_connector import SnowflakeConnector
from connectors.databricks_connector import DatabricksConnector
from core.exceptions import DataValidationError, TransformationError
import logging
import sys

class ShippingLogisticsPipeline:
    def __init__(self):
        self.spark_config = SparkConfig(
            app_name="ShippingLogisticsETL",
            master="local[*]",
            configs={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skew.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.databricks.delta.retentionDurationCheck.enabled": "false",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
            }
        )
        
        self.pipeline_config = PipelineConfig(
            adls_config=ADLSConfig(
                storage_account="shippinglogistics",
                container="raw-data"
            ),
            snowflake_config=SnowflakeConfig(
                account="xyz12345",
                user="shipping_etl_user",
                warehouse="SHIPPING_WH",
                database="SHIPPING_DB",
                schema="LOGISTICS",
                role="ETL_ROLE"
            ),
            input_paths={
                'shipments': 'bronze/shipments',
                'customers': 'bronze/customers',
                'carriers': 'bronze/carriers'
            },
            output_paths={
                DataLayer.SILVER: 'silver',
                DataLayer.GOLD: 'gold'
            },
            table_configs={
                'shipments': TableConfig(
                    table_name="fact_shipments",
                    primary_key=["shipment_id"],
                    partition_columns=["shipment_month"],
                    zorder_columns=["customer_id", "carrier_id"]
                ),
                'customers': TableConfig(
                    table_name="dim_customers",
                    primary_key=["customer_id"],
                    partition_columns=["customer_segment"],
                    zorder_columns=["customer_tier"]
                )
            }
        )
        
        self.spark_manager = SparkSessionManager(self.spark_config)
        self.spark = None
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('shipping_pipeline.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def initialize_spark(self):
        self.logger.info("Initializing Spark Session...")
        self.spark = self.spark_manager.create_session()
        self.logger.info("Spark Session initialized successfully")
    
    def run_pipeline(self):
        try:
            self.setup_logging()
            self.initialize_spark()
            
            self.logger.info("Starting Shipping Logistics Pipeline...")
            
            # Step 1: Data Ingestion
            self.logger.info("Step 1: Data Ingestion")
            ingestion_processor = DataIngestionProcessor(self.spark, self.pipeline_config)
            raw_data = ingestion_processor.ingest_all_data()
            
            # Step 2: Data Validation
            self.logger.info("Step 2: Data Validation")
            validation_processor = DataValidationProcessor(self.spark, self.pipeline_config)
            
            # Validate shipments data
            validation_processor.validate_completeness(
                raw_data['shipments'], 
                ['shipment_id', 'customer_id', 'shipment_date']
            )
            validation_processor.validate_uniqueness(
                raw_data['shipments'], 
                ['shipment_id']
            )
            
            # Step 3: Data Transformation
            self.logger.info("Step 3: Data Transformation")
            transformation_processor = DataTransformationProcessor(self.spark, self.pipeline_config)
            
            # Transform shipments data
            transformed_shipments = transformation_processor.add_derived_columns(raw_data['shipments'])
            transformed_shipments = transformation_processor.handle_missing_values(transformed_shipments)
            
            # Step 4: Data Enrichment
            self.logger.info("Step 4: Data Enrichment")
            enrichment_processor = DataEnrichmentProcessor(self.spark, self.pipeline_config)
            
            enriched_shipments = enrichment_processor.enrich_with_geographic_data(transformed_shipments)
            enriched_customers = enrichment_processor.enrich_with_customer_segment(raw_data['customers'])
            
            # Step 5: Advanced Transformations
            self.logger.info("Step 5: Advanced Transformations")
            
            # Shipment transformations
            shipment_transformer = ShipmentTransformer(self.spark, self.pipeline_config)
            shipment_metrics = shipment_transformer.calculate_shipment_metrics(enriched_shipments)
            shipment_summary = shipment_transformer.create_shipment_summary(shipment_metrics)
            
            # Customer transformations
            customer_transformer = CustomerTransformer(self.spark, self.pipeline_config)
            customer_clv = customer_transformer.create_customer_lifetime_value(
                shipment_metrics, enriched_customers
            )
            segmented_customers = customer_transformer.segment_customers(customer_clv)
            
            # Route transformations
            route_transformer = RouteTransformer(self.spark, self.pipeline_config)
            route_efficiency = route_transformer.calculate_route_efficiency(shipment_metrics)
            popular_routes = route_transformer.identify_popular_routes(shipment_metrics)
            
            # Financial transformations
            financial_transformer = FinancialTransformer(self.spark, self.pipeline_config)
            revenue_metrics = financial_transformer.calculate_revenue_metrics(
                shipment_metrics, raw_data['carriers']
            )
            
            # Step 6: Data Quality Checks
            self.logger.info("Step 6: Data Quality Checks")
            quality_processor = DataQualityProcessor(self.spark, self.pipeline_config)
            
            quality_checks = {
                'shipments': quality_processor.run_data_quality_checks(shipment_metrics, 'shipments'),
                'customers': quality_processor.run_data_quality_checks(segmented_customers, 'customers'),
                'routes': quality_processor.run_data_quality_checks(route_efficiency, 'routes')
            }
            
            # Step 7: Write to Silver Layer (ADLS)
            self.logger.info("Step 7: Writing to Silver Layer")
            adls_connector = ADLSConnector(self.spark, self.pipeline_config)
            
            # Write transformed data to silver layer
            adls_connector.write_to_adls(
                shipment_metrics,
                f"{self.pipeline_config.output_paths[DataLayer.SILVER]}/shipments",
                "delta",
                "overwrite",
                ["shipment_month"]
            )
            
            adls_connector.write_to_adls(
                segmented_customers,
                f"{self.pipeline_config.output_paths[DataLayer.SILVER]}/customers",
                "delta",
                "overwrite",
                ["customer_segment"]
            )
            
            # Step 8: Write to Gold Layer (Aggregated Data)
            self.logger.info("Step 8: Writing to Gold Layer")
            
            adls_connector.write_to_adls(
                shipment_summary,
                f"{self.pipeline_config.output_paths[DataLayer.GOLD]}/shipment_summary",
                "delta",
                "overwrite",
                ["shipment_month"]
            )
            
            adls_connector.write_to_adls(
                revenue_metrics,
                f"{self.pipeline_config.output_paths[DataLayer.GOLD]}/revenue_metrics",
                "delta",
                "overwrite",
                ["shipment_month"]
            )
            
            # Step 9: Downstream to Snowflake
            self.logger.info("Step 9: Downstream to Snowflake")
            snowflake_connector = SnowflakeConnector(self.spark, self.pipeline_config)
            
            # Write key tables to Snowflake
            snowflake_connector.write_to_snowflake(
                shipment_metrics, "fact_shipments", "overwrite"
            )
            
            snowflake_connector.write_to_snowflake(
                segmented_customers, "dim_customers", "overwrite"
            )
            
            snowflake_connector.write_to_snowflake(
                route_efficiency, "dim_routes", "overwrite"
            )
            
            # Step 10: Execute Snowflake Queries for Reporting
            self.logger.info("Step 10: Executing Snowflake Queries")
            
            # Create materialized views in Snowflake
            reporting_queries = [
                """
                CREATE OR REPLACE TABLE shipping_kpi_dashboard AS
                SELECT 
                    customer_tier,
                    COUNT(*) as customer_count,
                    AVG(clv_score) as avg_clv,
                    SUM(total_shipments) as total_shipments
                FROM dim_customers
                GROUP BY customer_tier
                """,
                """
                CREATE OR REPLACE TABLE route_performance AS
                SELECT 
                    origin_port,
                    destination_port,
                    route_volume,
                    route_efficiency_score,
                    avg_route_transit_time
                FROM dim_routes
                WHERE route_volume > 10
                ORDER BY route_efficiency_score DESC
                """
            ]
            
            for query in reporting_queries:
                snowflake_connector.execute_snowflake_query(query)
            
            self.logger.info("Shipping Logistics Pipeline completed successfully!")
            
        except DataValidationError as e:
            self.logger.error(f"Data validation error: {str(e)}")
            raise
        except TransformationError as e:
            self.logger.error(f"Transformation error: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in pipeline: {str(e)}")
            raise
        finally:
            if self.spark:
                self.logger.info("Stopping Spark Session...")
                self.spark_manager.stop_session()
    
    def run_streaming_pipeline(self):
        """Optional streaming pipeline for real-time data"""
        try:
            self.logger.info("Starting Streaming Pipeline...")
            
            # Read streaming data from Kafka or Event Hubs
            streaming_df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "your-kafka-servers") \
                .option("subscribe", "shipment-events") \
                .load()
            
            # Process streaming data
            processed_stream = streaming_df.selectExpr("CAST(value AS STRING)") \
                .selectExpr("JSON_VALUE(value, '$.shipment_id') as shipment_id",
                          "JSON_VALUE(value, '$.status') as status",
                          "JSON_VALUE(value, '$.timestamp') as event_timestamp")
            
            # Write to Delta table with foreachBatch
            query = processed_stream.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", "/checkpoints/shipment_events") \
                .start("/delta/shipment_events")
            
            query.awaitTermination()
            
        except Exception as e:
            self.logger.error(f"Streaming pipeline error: {str(e)}")
            raise

if __name__ == "__main__":
    pipeline = ShippingLogisticsPipeline()
    pipeline.run_pipeline()
    
    # Uncomment to run streaming pipeline
    # pipeline.run_streaming_pipeline()