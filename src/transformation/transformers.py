from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging
from typing import List, Dict

class RetailTransformations:
    """Business transformations for retail analytics"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def create_customer_360_view(self, customers_df: DataFrame, orders_df: DataFrame, 
                                order_items_df: DataFrame) -> DataFrame:
        """Create comprehensive customer view with purchase metrics"""
        self.logger.info("Creating customer 360 view")
        
        # Calculate order metrics per customer
        customer_orders = orders_df.filter(col("status") != "Cancelled") \
            .groupBy("customer_id") \
            .agg(
                count("order_id").alias("total_orders"),
                sum("total_amount").alias("lifetime_value"),
                avg("total_amount").alias("avg_order_value"),
                max("order_date").alias("last_order_date"),
                min("order_date").alias("first_order_date"),
                countDistinct("payment_method").alias("payment_methods_used")
            )
        
        # Calculate customer recency
        current_date = current_timestamp()
        customer_orders = customer_orders.withColumn(
            "days_since_last_order",
            datediff(current_date, col("last_order_date"))
        )
        
        # Join with customer data
        customer_360 = customers_df.join(
            customer_orders,
            on="customer_id",
            how="left"
        )
        
        # Add customer segmentation
        customer_360 = customer_360.withColumn(
            "customer_segment",
            when(col("lifetime_value") >= 5000, "VIP")
            .when(col("lifetime_value") >= 2000, "High Value")
            .when(col("lifetime_value") >= 500, "Regular")
            .otherwise("Low Value")
        )
        
        # Add activity status
        customer_360 = customer_360.withColumn(
            "activity_status",
            when(col("days_since_last_order").isNull(), "Never Purchased")
            .when(col("days_since_last_order") <= 30, "Active")
            .when(col("days_since_last_order") <= 90, "At Risk")
            .when(col("days_since_last_order") <= 180, "Dormant")
            .otherwise("Lost")
        )
        
        return customer_360
    
    def calculate_product_performance(self, products_df: DataFrame, order_items_df: DataFrame,
                                    orders_df: DataFrame) -> DataFrame:
        """Calculate product performance metrics"""
        self.logger.info("Calculating product performance")
        
        # Filter completed orders
        completed_orders = orders_df.filter(col("status") == "Completed").select("order_id")
        
        # Join with order items
        product_sales = order_items_df.join(
            completed_orders,
            on="order_id",
            how="inner"
        )
        
        # Calculate product metrics
        product_metrics = product_sales.groupBy("product_id") \
            .agg(
                sum("quantity").alias("units_sold"),
                sum("line_total").alias("revenue"),
                count("order_id").alias("times_ordered"),
                avg("unit_price").alias("avg_selling_price"),
                avg("discount_percent").alias("avg_discount_percent")
            )
        
        # Join with product data
        product_performance = products_df.join(
            product_metrics,
            on="product_id",
            how="left"
        )
        
        # Calculate profit metrics
        product_performance = product_performance.withColumn(
            "profit_per_unit",
            col("price") - col("cost")
        ).withColumn(
            "total_profit",
            col("units_sold") * (col("avg_selling_price") * (1 - col("avg_discount_percent") / 100) - col("cost"))
        ).withColumn(
            "profit_margin",
            ((col("price") - col("cost")) / col("price") * 100)
        )
        
        # Add inventory metrics
        product_performance = product_performance.withColumn(
            "inventory_turnover",
            when(col("stock_quantity") > 0, col("units_sold") / col("stock_quantity"))
            .otherwise(0)
        ).withColumn(
            "stock_status",
            when(col("stock_quantity") == 0, "Out of Stock")
            .when(col("stock_quantity") < 50, "Low Stock")
            .when(col("stock_quantity") > 500, "Overstock")
            .otherwise("Normal")
        )
        
        return product_performance
    
    def create_sales_summary(self, orders_df: DataFrame, order_items_df: DataFrame) -> DataFrame:
        """Create sales summary by various dimensions"""
        self.logger.info("Creating sales summary")
        
        # Join orders with items
        sales_data = orders_df.filter(col("status") == "Completed") \
            .join(order_items_df, on="order_id", how="inner")
        
        # Extract date components
        sales_data = sales_data \
            .withColumn("order_year", year("order_date")) \
            .withColumn("order_month", month("order_date")) \
            .withColumn("order_day", dayofmonth("order_date")) \
            .withColumn("order_weekday", dayofweek("order_date")) \
            .withColumn("order_quarter", quarter("order_date"))
        
        # Daily sales summary
        daily_summary = sales_data.groupBy(
            "order_year", "order_month", "order_day"
        ).agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("line_total").alias("total_revenue"),
            avg("line_total").alias("avg_order_value"),
            sum("quantity").alias("total_units_sold")
        ).orderBy("order_year", "order_month", "order_day")
        
        # Add moving averages
        window_spec = Window.orderBy("order_year", "order_month", "order_day") \
            .rowsBetween(-6, 0)
        
        daily_summary = daily_summary.withColumn(
            "revenue_7day_avg",
            avg("total_revenue").over(window_spec)
        ).withColumn(
            "orders_7day_avg",
            avg("total_orders").over(window_spec)
        )
        
        return daily_summary
    
    def analyze_customer_behavior(self, customers_df: DataFrame, orders_df: DataFrame,
                                order_items_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """Analyze customer purchasing behavior and preferences"""
        self.logger.info("Analyzing customer behavior")
        
        # Get customer order details
        customer_purchases = orders_df.filter(col("status") == "Completed") \
            .join(order_items_df, on="order_id") \
            .join(products_df.select("product_id", "category", "brand"), on="product_id")
        
        # Calculate favorite categories per customer
        category_preferences = customer_purchases.groupBy("customer_id", "category") \
            .agg(
                count("order_id").alias("category_orders"),
                sum("line_total").alias("category_spend")
            )
        
        # Get top category per customer
        window_spec = Window.partitionBy("customer_id").orderBy(desc("category_spend"))
        
        top_categories = category_preferences \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") == 1) \
            .select(
                col("customer_id"),
                col("category").alias("favorite_category"),
                col("category_spend").alias("favorite_category_spend")
            )
        
        # Calculate purchase patterns
        purchase_patterns = customer_purchases.groupBy("customer_id") \
            .agg(
                countDistinct("category").alias("categories_purchased"),
                countDistinct("brand").alias("brands_purchased"),
                avg("discount_percent").alias("avg_discount_used"),
                collect_list("payment_method").alias("payment_methods")
            )
        
        # Get most used payment method
        purchase_patterns = purchase_patterns.withColumn(
            "preferred_payment_method",
            expr("sort_array(array_distinct(payment_methods))[0]")
        ).drop("payment_methods")
        
        # Combine behavior metrics
        customer_behavior = customers_df.select("customer_id") \
            .join(top_categories, on="customer_id", how="left") \
            .join(purchase_patterns, on="customer_id", how="left")
        
        return customer_behavior