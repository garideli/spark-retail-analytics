from pyspark.sql import DataFrame, SparkSession
import logging
import os
import shutil
from typing import Dict, Optional
from datetime import datetime
import glob

class CustomFileWriter:
    """Handles writing DataFrames with custom file naming"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def write_single_csv(self, df: DataFrame, output_path: str, file_name: str,
                        delimiter: str = ",", header: bool = True) -> str:
        """
        Write DataFrame as a single CSV file with custom name.
        This handles Spark's partitioned output by coalescing and renaming.
        """
        self.logger.info(f"Writing single CSV file: {file_name}")
        
        # Create temp directory
        temp_dir = f"{output_path}/_temp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            # Coalesce to single partition and write
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", str(header).lower()) \
                .option("delimiter", delimiter) \
                .csv(temp_dir)
            
            # Find the part file
            part_files = glob.glob(f"{temp_dir}/part-*.csv")
            if not part_files:
                # Try without .csv extension
                part_files = glob.glob(f"{temp_dir}/part-*")
            
            if part_files:
                # Move and rename the file
                final_path = os.path.join(output_path, file_name)
                os.makedirs(os.path.dirname(final_path), exist_ok=True)
                shutil.move(part_files[0], final_path)
                self.logger.info(f"Successfully wrote file: {final_path}")
                
                # Clean up temp directory
                shutil.rmtree(temp_dir)
                
                return final_path
            else:
                raise Exception("No part files found in temporary directory")
                
        except Exception as e:
            self.logger.error(f"Error writing single CSV: {str(e)}")
            # Clean up on error
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise
    
    def write_partitioned_csv(self, df: DataFrame, output_path: str, 
                            partition_cols: list, file_prefix: str,
                            delimiter: str = ",", header: bool = True) -> str:
        """
        Write DataFrame as partitioned CSV files with custom naming.
        Each partition gets a custom name based on partition values.
        """
        self.logger.info(f"Writing partitioned CSV files with prefix: {file_prefix}")
        
        # Create temp directory
        temp_dir = f"{output_path}/_temp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            # Write partitioned data
            df.write \
                .mode("overwrite") \
                .option("header", str(header).lower()) \
                .option("delimiter", delimiter) \
                .partitionBy(*partition_cols) \
                .csv(temp_dir)
            
            # Process each partition directory
            processed_files = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file.startswith("part-"):
                        # Extract partition values from path
                        partition_info = self._extract_partition_info(root, temp_dir, partition_cols)
                        
                        # Create custom filename
                        partition_suffix = "_".join([f"{k}_{v}" for k, v in partition_info.items()])
                        custom_name = f"{file_prefix}_{partition_suffix}.csv"
                        
                        # Create output directory structure
                        relative_path = os.path.relpath(root, temp_dir)
                        final_dir = os.path.join(output_path, relative_path)
                        os.makedirs(final_dir, exist_ok=True)
                        
                        # Move and rename file
                        source_path = os.path.join(root, file)
                        final_path = os.path.join(final_dir, custom_name)
                        shutil.move(source_path, final_path)
                        processed_files.append(final_path)
            
            # Clean up temp directory
            shutil.rmtree(temp_dir)
            
            self.logger.info(f"Successfully wrote {len(processed_files)} partitioned files")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Error writing partitioned CSV: {str(e)}")
            # Clean up on error
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise
    
    def write_with_timestamp(self, df: DataFrame, output_path: str, 
                           base_name: str, format: str = "csv",
                           **options) -> str:
        """Write DataFrame with timestamp in filename"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format.lower() == "csv":
            file_name = f"{base_name}_{timestamp}.csv"
            return self.write_single_csv(df, output_path, file_name, **options)
        elif format.lower() == "parquet":
            file_name = f"{base_name}_{timestamp}.parquet"
            return self.write_single_parquet(df, output_path, file_name)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def write_single_parquet(self, df: DataFrame, output_path: str, file_name: str) -> str:
        """Write DataFrame as a single Parquet file with custom name"""
        self.logger.info(f"Writing single Parquet file: {file_name}")
        
        # Create temp directory
        temp_dir = f"{output_path}/_temp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            # Coalesce to single partition and write
            df.coalesce(1).write \
                .mode("overwrite") \
                .parquet(temp_dir)
            
            # Find the part file
            part_files = glob.glob(f"{temp_dir}/part-*.parquet")
            
            if part_files:
                # Move and rename the file
                final_path = os.path.join(output_path, file_name)
                os.makedirs(os.path.dirname(final_path), exist_ok=True)
                shutil.move(part_files[0], final_path)
                self.logger.info(f"Successfully wrote file: {final_path}")
                
                # Clean up temp directory
                shutil.rmtree(temp_dir)
                
                return final_path
            else:
                raise Exception("No part files found in temporary directory")
                
        except Exception as e:
            self.logger.error(f"Error writing single Parquet: {str(e)}")
            # Clean up on error
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise
    
    def write_json_lines(self, df: DataFrame, output_path: str, file_name: str) -> str:
        """Write DataFrame as JSON Lines file with custom name"""
        self.logger.info(f"Writing JSON Lines file: {file_name}")
        
        # Create temp directory
        temp_dir = f"{output_path}/_temp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            # Coalesce to single partition and write
            df.coalesce(1).write \
                .mode("overwrite") \
                .json(temp_dir)
            
            # Find the part file
            part_files = glob.glob(f"{temp_dir}/part-*.json")
            
            if part_files:
                # Move and rename the file
                final_path = os.path.join(output_path, file_name)
                os.makedirs(os.path.dirname(final_path), exist_ok=True)
                shutil.move(part_files[0], final_path)
                self.logger.info(f"Successfully wrote file: {final_path}")
                
                # Clean up temp directory
                shutil.rmtree(temp_dir)
                
                return final_path
            else:
                raise Exception("No part files found in temporary directory")
                
        except Exception as e:
            self.logger.error(f"Error writing JSON Lines: {str(e)}")
            # Clean up on error
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise
    
    def _extract_partition_info(self, path: str, base_path: str, 
                               partition_cols: list) -> Dict[str, str]:
        """Extract partition column values from directory path"""
        relative_path = os.path.relpath(path, base_path)
        parts = relative_path.split(os.sep)
        
        partition_info = {}
        for part in parts:
            if "=" in part:
                key, value = part.split("=", 1)
                if key in partition_cols:
                    partition_info[key] = value
        
        return partition_info