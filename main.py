import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
import logging
from datetime import datetime
import argparse

from src.utils.spark_utils import create_spark_session, setup_logging
from src.ingestion.data_loader import DataLoader
from src.ingestion.schemas import DataSchemas
from src.transformation.transformers import RetailTransformations
from src.quality.data_quality import DataQualityChecker
from src.output.custom_writer import CustomFileWriter

def main(args):
    """Main orchestration function"""
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Retail Analytics Pipeline")
    
    # Create Spark session
    spark = create_spark_session("RetailAnalyticsPipeline")
    
    try:
        # Initialize components
        loader = DataLoader(spark)
        schemas = DataSchemas()
        transformer = RetailTransformations(spark)
        quality_checker = DataQualityChecker(spark)
        writer = CustomFileWriter(spark)
        
        # Step 1: Data Ingestion
        logger.info("Step 1: Loading data")
        
        customers_df = loader.load_csv(
            os.path.join(args.input_path, "customers.csv"),
            schema=schemas.get_customer_schema()
        )
        
        products_df = loader.load_csv(
            os.path.join(args.input_path, "products.csv"),
            schema=schemas.get_product_schema()
        )
        
        orders_df = loader.load_csv(
            os.path.join(args.input_path, "orders.csv"),
            schema=schemas.get_order_schema()
        )
        
        order_items_df = loader.load_csv(
            os.path.join(args.input_path, "order_items.csv"),
            schema=schemas.get_order_item_schema()
        )
        
        # Show raw data samples
        logger.info("Raw Data Preview:")
        print("\n=== CUSTOMERS DATA ===")
        customers_df.show(5, truncate=False)
        
        print("\n=== PRODUCTS DATA ===")
        products_df.show(5, truncate=False)
        
        print("\n=== ORDERS DATA ===")
        orders_df.show(5, truncate=False)
        
        print("\n=== ORDER ITEMS DATA ===")
        order_items_df.show(5, truncate=False)
        
        # Step 2: Data Quality Checks
        logger.info("Step 2: Running data quality checks")
        
        # Check for nulls in critical columns
        quality_checker.check_nulls(customers_df, ["customer_id", "email"])
        quality_checker.check_nulls(products_df, ["product_id", "price"])
        quality_checker.check_nulls(orders_df, ["order_id", "customer_id"])
        
        # Check for duplicates
        quality_checker.check_duplicates(customers_df, ["customer_id"])
        quality_checker.check_duplicates(products_df, ["product_id"])
        quality_checker.check_duplicates(orders_df, ["order_id"])
        
        # Check referential integrity
        quality_checker.check_referential_integrity(orders_df, customers_df, "customer_id")
        quality_checker.check_referential_integrity(order_items_df, orders_df, "order_id")
        quality_checker.check_referential_integrity(order_items_df, products_df, "product_id")
        
        # Step 3: Data Transformations
        logger.info("Step 3: Applying transformations")
        
        # Create customer 360 view
        customer_360_df = transformer.create_customer_360_view(
            customers_df, orders_df, order_items_df
        )
        
        # Calculate product performance
        product_performance_df = transformer.calculate_product_performance(
            products_df, order_items_df, orders_df
        )
        
        # Create sales summary
        sales_summary_df = transformer.create_sales_summary(
            orders_df, order_items_df
        )
        
        # Analyze customer behavior
        customer_behavior_df = transformer.analyze_customer_behavior(
            customers_df, orders_df, order_items_df, products_df
        )
        
        # Show transformed data samples
        logger.info("Transformed Data Preview:")
        print("\n=== CUSTOMER 360 VIEW ===")
        customer_360_df.show(5, truncate=False)
        
        print("\n=== PRODUCT PERFORMANCE ===")
        product_performance_df.show(5, truncate=False)
        
        print("\n=== SALES SUMMARY ===")
        sales_summary_df.show(5, truncate=False)
        
        print("\n=== CUSTOMER BEHAVIOR ANALYSIS ===")
        customer_behavior_df.show(5, truncate=False)
        
        # Step 4: Write outputs with custom file names
        logger.info("Step 4: Writing outputs")
        
        # Write customer 360 view as single CSV with custom name
        writer.write_single_csv(
            customer_360_df,
            args.output_path,
            "customer_360_view_analysis.csv"
        )
        
        # Write product performance with timestamp
        writer.write_with_timestamp(
            product_performance_df,
            args.output_path,
            "product_performance",
            format="csv"
        )
        
        # Write sales summary partitioned by year and month
        sales_summary_df_partitioned = sales_summary_df.select(
            "*",
            col("order_year").alias("year"),
            col("order_month").alias("month")
        )
        
        writer.write_partitioned_csv(
            sales_summary_df_partitioned,
            os.path.join(args.output_path, "sales_summary_partitioned"),
            ["year", "month"],
            "sales_summary"
        )
        
        # Write customer behavior as Parquet
        writer.write_single_parquet(
            customer_behavior_df,
            args.output_path,
            "customer_behavior_analysis.parquet"
        )
        
        # Generate and save quality reports
        if args.generate_reports:
            logger.info("Generating quality reports")
            
            # Customer 360 quality report
            customer_report = quality_checker.generate_quality_report(
                customer_360_df,
                "customer_360_view"
            )
            
            # Save report as JSON
            spark.sparkContext.parallelize([customer_report]).coalesce(1) \
                .map(lambda x: str(x)) \
                .saveAsTextFile(os.path.join(args.output_path, "quality_reports"))
        
        logger.info("Pipeline completed successfully!")
        
        # Show sample results if requested
        if args.show_samples:
            logger.info("Sample results:")
            logger.info("\nCustomer 360 View (Top 5 by lifetime value):")
            customer_360_df.orderBy(col("lifetime_value").desc()).show(5, truncate=False)
            
            logger.info("\nTop 5 Products by Revenue:")
            product_performance_df.orderBy(col("revenue").desc()).show(5, truncate=False)
            
            logger.info("\nRecent Sales Summary:")
            sales_summary_df.orderBy(col("order_year").desc(), col("order_month").desc()).show(10)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail Analytics Pipeline")
    parser.add_argument(
        "--input-path",
        default="data/raw",
        help="Path to input data files"
    )
    parser.add_argument(
        "--output-path",
        default="data/output",
        help="Path for output files"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    parser.add_argument(
        "--generate-reports",
        action="store_true",
        help="Generate data quality reports"
    )
    parser.add_argument(
        "--show-samples",
        action="store_true",
        help="Show sample results at the end"
    )
    
    args = parser.parse_args()
    
    from pyspark.sql.functions import col
    main(args)