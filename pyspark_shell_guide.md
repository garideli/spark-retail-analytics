# PySpark Shell Guide for Interactive DataFrame Access

## Quick Start

### 1. Launch PySpark Shell
```bash
cd /Users/drupa/projects/spark-retail-analytics
pyspark
```

### 2. Load Data into DataFrames

Once in the PySpark shell, run:

```python
# Load raw data
customers = spark.read.csv("data/raw/customers.csv", header=True, inferSchema=True)
products = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)
orders = spark.read.csv("data/raw/orders.csv", header=True, inferSchema=True)
order_items = spark.read.csv("data/raw/order_items.csv", header=True, inferSchema=True)

# Load processed data
customer_360 = spark.read.csv("data/output/customer_360_view_analysis.csv", header=True, inferSchema=True)
customer_behavior = spark.read.parquet("data/output/customer_behavior_analysis.parquet")
```

### 3. Explore DataFrames

```python
# Show first 20 rows
customers.show()

# Show with specific number of rows
products.show(10)

# Display schema
orders.printSchema()

# Count rows
print(f"Total customers: {customers.count()}")

# Show specific columns
customers.select("customer_id", "first_name", "city", "total_spent").show()

# Filter data
customers.filter(customers.loyalty_tier == "Gold").show()

# Sort data
orders.orderBy("total_amount", ascending=False).show(10)
```

### 4. SQL Queries

```python
# Register DataFrames as temporary views
customers.createOrReplaceTempView("customers")
products.createOrReplaceTempView("products")
orders.createOrReplaceTempView("orders")
order_items.createOrReplaceTempView("order_items")

# Run SQL queries
spark.sql("""
    SELECT state, COUNT(*) as customer_count
    FROM customers
    GROUP BY state
    ORDER BY customer_count DESC
""").show()

# Join tables
spark.sql("""
    SELECT c.first_name, c.last_name, o.order_date, o.total_amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.total_amount > 1000
    ORDER BY o.total_amount DESC
""").show()
```

### 5. Convert to Pandas for Visualization

```python
# Convert small DataFrame to Pandas
df_pandas = customers.limit(100).toPandas()

# Now you can use matplotlib/seaborn
import matplotlib.pyplot as plt
df_pandas['loyalty_tier'].value_counts().plot(kind='bar')
plt.show()
```

### 6. Advanced Operations

```python
# Group by and aggregate
from pyspark.sql import functions as F

# Average order value by customer
orders.groupBy("customer_id") \
    .agg(F.avg("total_amount").alias("avg_order_value"),
         F.count("order_id").alias("total_orders")) \
    .orderBy("avg_order_value", ascending=False) \
    .show()

# Window functions
from pyspark.sql.window import Window

# Rank products by revenue
windowSpec = Window.partitionBy("category").orderBy(F.desc("revenue"))
product_df = spark.read.csv("data/output/product_performance_*.csv", header=True, inferSchema=True)
product_df.withColumn("rank", F.rank().over(windowSpec)) \
    .filter(F.col("rank") <= 5) \
    .select("category", "product_name", "revenue", "rank") \
    .show()
```

## Tips

1. **Tab Completion**: Use Tab key for auto-completion of methods
2. **Help**: Use `help(dataframe.method)` for documentation
3. **Exit**: Type `exit()` or Ctrl+D to leave PySpark shell
4. **Save Results**: 
   ```python
   # Save to CSV
   result.write.csv("output/my_analysis.csv", header=True, mode="overwrite")
   
   # Save to Parquet
   result.write.parquet("output/my_analysis.parquet", mode="overwrite")
   ```