#!/usr/bin/env python3
"""
Interactive Data Viewer for Spark Retail Analytics
Allows you to explore dataframes interactively
"""

import pandas as pd
from pyspark.sql import SparkSession
import os
from tabulate import tabulate

def display_dataframe(df, name, num_rows=10):
    """Display a dataframe with formatting"""
    print(f"\n{'='*80}")
    print(f"{name}")
    print(f"{'='*80}")
    print(f"Shape: {df.shape if hasattr(df, 'shape') else f'({df.count()} rows)'}")
    print(tabulate(df.head(num_rows) if hasattr(df, 'head') else df.limit(num_rows).toPandas(), 
                   headers='keys', tablefmt='grid', showindex=False))

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("InteractiveDataViewer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    while True:
        print("\n" + "="*80)
        print("SPARK RETAIL ANALYTICS - INTERACTIVE DATA VIEWER")
        print("="*80)
        print("\nAvailable Datasets:")
        print("\nRAW DATA:")
        print("1. Customers")
        print("2. Products") 
        print("3. Orders")
        print("4. Order Items")
        print("\nPROCESSED DATA:")
        print("5. Customer 360 View")
        print("6. Product Performance")
        print("7. Customer Behavior (Parquet)")
        print("\nOPTIONS:")
        print("8. Show SQL query interface")
        print("9. Export selected data")
        print("0. Exit")
        
        choice = input("\nSelect dataset (0-9): ").strip()
        
        if choice == '0':
            print("Exiting...")
            spark.stop()
            break
            
        try:
            if choice == '1':
                df = pd.read_csv(f"{base_path}/data/raw/customers.csv")
                display_dataframe(df, "CUSTOMERS DATA")
                
                # Additional options
                while True:
                    print("\nOptions: (f)ilter, (s)ort, (g)roup by, (b)ack")
                    opt = input("Select option: ").strip().lower()
                    if opt == 'b':
                        break
                    elif opt == 'f':
                        col = input("Filter by column: ")
                        val = input("Filter value contains: ")
                        filtered = df[df[col].astype(str).str.contains(val, na=False)]
                        display_dataframe(filtered, f"FILTERED CUSTOMERS ('{col}' contains '{val}')")
                    elif opt == 's':
                        col = input("Sort by column: ")
                        ascending = input("Ascending? (y/n): ").lower() == 'y'
                        sorted_df = df.sort_values(col, ascending=ascending)
                        display_dataframe(sorted_df, f"SORTED CUSTOMERS BY {col}")
                    elif opt == 'g':
                        col = input("Group by column: ")
                        grouped = df.groupby(col).size().reset_index(name='count')
                        display_dataframe(grouped, f"CUSTOMERS GROUPED BY {col}")
                        
            elif choice == '2':
                df = pd.read_csv(f"{base_path}/data/raw/products.csv")
                display_dataframe(df, "PRODUCTS DATA")
                
            elif choice == '3':
                df = pd.read_csv(f"{base_path}/data/raw/orders.csv")
                display_dataframe(df, "ORDERS DATA")
                
            elif choice == '4':
                df = pd.read_csv(f"{base_path}/data/raw/order_items.csv")
                display_dataframe(df, "ORDER ITEMS DATA")
                
            elif choice == '5':
                df = pd.read_csv(f"{base_path}/data/output/customer_360_view_analysis.csv")
                display_dataframe(df, "CUSTOMER 360 VIEW")
                
            elif choice == '6':
                # Find the latest product performance file
                output_dir = f"{base_path}/data/output"
                files = [f for f in os.listdir(output_dir) if f.startswith("product_performance_")]
                if files:
                    latest_file = sorted(files)[-1]
                    df = pd.read_csv(f"{output_dir}/{latest_file}")
                    display_dataframe(df, "PRODUCT PERFORMANCE")
                    
            elif choice == '7':
                # Read parquet file using Spark
                df_spark = spark.read.parquet(f"{base_path}/data/output/customer_behavior_analysis.parquet")
                df = df_spark.limit(20).toPandas()
                display_dataframe(df, "CUSTOMER BEHAVIOR ANALYSIS")
                
            elif choice == '8':
                print("\nSQL Query Interface")
                print("Available tables: customers, products, orders, order_items")
                
                # Register tables
                customers_df = spark.read.csv(f"{base_path}/data/raw/customers.csv", header=True, inferSchema=True)
                products_df = spark.read.csv(f"{base_path}/data/raw/products.csv", header=True, inferSchema=True)
                orders_df = spark.read.csv(f"{base_path}/data/raw/orders.csv", header=True, inferSchema=True)
                order_items_df = spark.read.csv(f"{base_path}/data/raw/order_items.csv", header=True, inferSchema=True)
                
                customers_df.createOrReplaceTempView("customers")
                products_df.createOrReplaceTempView("products")
                orders_df.createOrReplaceTempView("orders")
                order_items_df.createOrReplaceTempView("order_items")
                
                while True:
                    query = input("\nEnter SQL query (or 'back' to return): ")
                    if query.lower() == 'back':
                        break
                    try:
                        result = spark.sql(query)
                        display_dataframe(result.limit(20).toPandas(), "QUERY RESULT")
                    except Exception as e:
                        print(f"Error: {e}")
                        
            elif choice == '9':
                print("\nExport Options:")
                print("1. Export to CSV")
                print("2. Export to Excel (requires openpyxl)")
                print("3. Export to JSON")
                
                export_choice = input("Select export format: ")
                dataset = input("Which dataset to export (1-7): ")
                
                # Load the selected dataset
                if dataset == '1':
                    df = pd.read_csv(f"{base_path}/data/raw/customers.csv")
                    name = "customers"
                elif dataset == '2':
                    df = pd.read_csv(f"{base_path}/data/raw/products.csv")
                    name = "products"
                # ... add other datasets
                
                if export_choice == '1':
                    filename = f"{name}_export.csv"
                    df.to_csv(filename, index=False)
                    print(f"Exported to {filename}")
                elif export_choice == '3':
                    filename = f"{name}_export.json"
                    df.to_json(filename, orient='records', indent=2)
                    print(f"Exported to {filename}")
                    
        except Exception as e:
            print(f"Error: {e}")
            
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()