from pyspark.sql.types import *

class DataSchemas:
    """Define schemas for all data sources"""
    
    @staticmethod
    def get_customer_schema() -> StructType:
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("registration_date", DateType(), True),
            StructField("loyalty_tier", StringType(), True),
            StructField("total_spent", DoubleType(), True)
        ])
    
    @staticmethod
    def get_product_schema() -> StructType:
        return StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("cost", DoubleType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("supplier_id", StringType(), True),
            StructField("weight", DoubleType(), True),
            StructField("is_active", BooleanType(), True)
        ])
    
    @staticmethod
    def get_order_schema() -> StructType:
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("shipping_city", StringType(), True),
            StructField("shipping_state", StringType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True)
        ])
    
    @staticmethod
    def get_order_item_schema() -> StructType:
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("line_total", DoubleType(), True),
            StructField("discount_percent", IntegerType(), True)
        ])