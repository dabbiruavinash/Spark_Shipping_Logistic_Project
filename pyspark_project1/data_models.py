from pyspark.sql.types import *
from datetime import datetime
from typing import Optional

# Schema definitions for shipping logistics
class ShippingSchemas:
    SHIPMENT_SCHEMA = StructType([
        StructField("shipment_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("origin_port", StringType(), True),
        StructField("destination_port", StringType(), True),
        StructField("shipment_date", TimestampType(), True),
        StructField("delivery_date", TimestampType(), True),
        StructField("weight_kg", DoubleType(), True),
        StructField("volume_cbm", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("carrier_id", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])
    
    CUSTOMER_SCHEMA = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("customer_type", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("credit_limit", DecimalType(10, 2), True)
    ])
    
    CARRIER_SCHEMA = StructType([
        StructField("carrier_id", StringType(), False),
        StructField("carrier_name", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("transit_time_days", IntegerType(), True),
        StructField("rate_per_kg", DecimalType(10, 2), True)
    ])