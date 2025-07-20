from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
import logging
from typing import Dict, List, Tuple
from datetime import datetime

class DataQualityChecker:
    """Perform data quality checks on DataFrames"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self.quality_results = []
    
    def check_nulls(self, df: DataFrame, columns: List[str] = None, 
                   threshold: float = 0.05) -> Tuple[bool, Dict]:
        """Check for null values in specified columns"""
        self.logger.info("Checking for null values")
        
        if columns is None:
            columns = df.columns
            
        results = {}
        passed = True
        
        total_rows = df.count()
        
        for column in columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = null_count / total_rows if total_rows > 0 else 0
            
            results[column] = {
                "null_count": null_count,
                "null_percentage": null_percentage * 100,
                "passed": null_percentage <= threshold
            }
            
            if null_percentage > threshold:
                passed = False
                self.logger.warning(f"Column {column} has {null_percentage*100:.2f}% nulls, exceeding threshold of {threshold*100}%")
        
        return passed, results
    
    def check_duplicates(self, df: DataFrame, key_columns: List[str]) -> Tuple[bool, Dict]:
        """Check for duplicate records based on key columns"""
        self.logger.info(f"Checking for duplicates on columns: {key_columns}")
        
        total_rows = df.count()
        distinct_rows = df.select(key_columns).distinct().count()
        duplicate_count = total_rows - distinct_rows
        
        results = {
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": (duplicate_count / total_rows * 100) if total_rows > 0 else 0,
            "passed": duplicate_count == 0
        }
        
        if duplicate_count > 0:
            self.logger.warning(f"Found {duplicate_count} duplicate records")
            
            # Get sample of duplicates
            duplicates_df = df.groupBy(key_columns).count() \
                .filter(col("count") > 1) \
                .limit(10)
            results["duplicate_samples"] = duplicates_df.collect()
        
        return results["passed"], results
    
    def check_data_freshness(self, df: DataFrame, date_column: str, 
                           max_days_old: int = 30) -> Tuple[bool, Dict]:
        """Check if data is fresh enough"""
        self.logger.info(f"Checking data freshness for column: {date_column}")
        
        current_date = datetime.now()
        
        # Get min and max dates
        date_stats = df.agg(
            min(date_column).alias("min_date"),
            max(date_column).alias("max_date")
        ).collect()[0]
        
        min_date = date_stats["min_date"]
        max_date = date_stats["max_date"]
        
        if max_date:
            days_old = (current_date - max_date).days
            passed = days_old <= max_days_old
        else:
            days_old = None
            passed = False
        
        results = {
            "min_date": min_date,
            "max_date": max_date,
            "days_old": days_old,
            "max_days_allowed": max_days_old,
            "passed": passed
        }
        
        if not passed:
            self.logger.warning(f"Data is {days_old} days old, exceeding threshold of {max_days_old} days")
        
        return passed, results
    
    def check_value_ranges(self, df: DataFrame, column_ranges: Dict[str, Tuple]) -> Tuple[bool, Dict]:
        """Check if numeric values are within expected ranges"""
        self.logger.info("Checking value ranges")
        
        results = {}
        passed = True
        
        for column, (min_val, max_val) in column_ranges.items():
            out_of_range = df.filter(
                (col(column) < min_val) | (col(column) > max_val)
            ).count()
            
            total_rows = df.count()
            
            results[column] = {
                "out_of_range_count": out_of_range,
                "out_of_range_percentage": (out_of_range / total_rows * 100) if total_rows > 0 else 0,
                "expected_range": (min_val, max_val),
                "passed": out_of_range == 0
            }
            
            if out_of_range > 0:
                passed = False
                self.logger.warning(f"Column {column} has {out_of_range} values outside range [{min_val}, {max_val}]")
        
        return passed, results
    
    def check_referential_integrity(self, df1: DataFrame, df2: DataFrame, 
                                  join_key: str) -> Tuple[bool, Dict]:
        """Check referential integrity between two DataFrames"""
        self.logger.info(f"Checking referential integrity on key: {join_key}")
        
        # Find orphaned records
        orphaned_records = df1.join(
            df2,
            df1[join_key] == df2[join_key],
            "left_anti"
        ).count()
        
        total_records = df1.count()
        
        results = {
            "total_records": total_records,
            "orphaned_records": orphaned_records,
            "orphaned_percentage": (orphaned_records / total_records * 100) if total_records > 0 else 0,
            "passed": orphaned_records == 0
        }
        
        if orphaned_records > 0:
            self.logger.warning(f"Found {orphaned_records} orphaned records")
        
        return results["passed"], results
    
    def generate_quality_report(self, df: DataFrame, df_name: str) -> Dict:
        """Generate comprehensive data quality report"""
        self.logger.info(f"Generating quality report for {df_name}")
        
        report = {
            "dataframe_name": df_name,
            "timestamp": datetime.now().isoformat(),
            "total_rows": df.count(),
            "total_columns": len(df.columns),
            "checks": {}
        }
        
        # Basic statistics
        report["basic_stats"] = {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "schema": df.schema.json()
        }
        
        # Column profiling
        column_profiles = {}
        for column in df.columns:
            col_type = dict(df.dtypes)[column]
            profile = {
                "data_type": col_type,
                "distinct_count": df.select(column).distinct().count(),
                "null_count": df.filter(col(column).isNull()).count()
            }
            
            # Add numeric statistics if applicable
            if col_type in ["double", "float", "int", "bigint"]:
                stats = df.select(
                    min(column).alias("min"),
                    max(column).alias("max"),
                    avg(column).alias("avg"),
                    stddev(column).alias("stddev")
                ).collect()[0]
                
                profile.update({
                    "min": stats["min"],
                    "max": stats["max"],
                    "avg": stats["avg"],
                    "stddev": stats["stddev"]
                })
            
            column_profiles[column] = profile
        
        report["column_profiles"] = column_profiles
        
        return report