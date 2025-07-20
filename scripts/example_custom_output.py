"""
Example script demonstrating custom file naming with PySpark
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.output.custom_writer import CustomFileWriter
from src.utils.spark_utils import create_spark_session

def main():
    # Create Spark session
    spark = create_spark_session("CustomFileNamingExample")
    
    # Create sample data
    data = [
        ("2024-01-15", "Electronics", "Laptop", 1200.00, 10),
        ("2024-01-15", "Electronics", "Phone", 800.00, 15),
        ("2024-01-16", "Clothing", "Shirt", 50.00, 25),
        ("2024-01-16", "Clothing", "Pants", 80.00, 20),
        ("2024-02-01", "Electronics", "Tablet", 600.00, 8),
        ("2024-02-01", "Home", "Lamp", 45.00, 30)
    ]
    
    columns = ["date", "category", "product", "price", "quantity"]
    df = spark.createDataFrame(data, columns)
    
    # Add calculated columns
    df = df.withColumn("total_value", col("price") * col("quantity")) \
           .withColumn("year", year("date")) \
           .withColumn("month", month("date"))
    
    # Initialize custom writer
    writer = CustomFileWriter(spark)
    
    print("Demonstrating Custom File Naming in PySpark\n")
    print("=" * 50)
    
    # Example 1: Single CSV with custom name
    print("\n1. Writing single CSV with custom name...")
    output_path = "../data/output/examples"
    os.makedirs(output_path, exist_ok=True)
    
    result1 = writer.write_single_csv(
        df=df,
        output_path=output_path,
        file_name="sales_data_january_2024.csv"
    )
    print(f"   Created: {result1}")
    
    # Example 2: CSV with timestamp
    print("\n2. Writing CSV with timestamp...")
    result2 = writer.write_with_timestamp(
        df=df,
        output_path=output_path,
        base_name="sales_report",
        format="csv"
    )
    print(f"   Created: {result2}")
    
    # Example 3: Parquet with custom name
    print("\n3. Writing Parquet with custom name...")
    result3 = writer.write_single_parquet(
        df=df,
        output_path=output_path,
        file_name="sales_data_compressed.parquet"
    )
    print(f"   Created: {result3}")
    
    # Example 4: Partitioned CSV with custom naming
    print("\n4. Writing partitioned CSV with custom names...")
    result4 = writer.write_partitioned_csv(
        df=df,
        output_path=os.path.join(output_path, "partitioned"),
        partition_cols=["year", "month"],
        file_prefix="monthly_sales"
    )
    print(f"   Created partitioned files in: {result4}")
    
    # Example 5: JSON Lines with custom name
    print("\n5. Writing JSON Lines with custom name...")
    result5 = writer.write_json_lines(
        df=df,
        output_path=output_path,
        file_name="sales_data.jsonl"
    )
    print(f"   Created: {result5}")
    
    print("\n" + "=" * 50)
    print("All examples completed successfully!")
    print("\nKey Takeaways:")
    print("- Spark normally creates part-00000 files in directories")
    print("- Custom writer coalesces to single partition and renames")
    print("- Supports CSV, Parquet, and JSON formats")
    print("- Can add timestamps for versioning")
    print("- Handles partitioned outputs with meaningful names")
    
    spark.stop()

if __name__ == "__main__":
    main()