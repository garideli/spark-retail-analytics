from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import logging
from typing import Dict, Optional
import os

class DataLoader:
    """Handles data ingestion from various sources"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        
    def load_csv(self, file_path: str, schema: Optional[StructType] = None, 
                 options: Optional[Dict[str, str]] = None) -> DataFrame:
        """Load CSV file with optional schema and options"""
        self.logger.info(f"Loading CSV file: {file_path}")
        
        reader = self.spark.read
        
        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")
            
        reader = reader.option("header", "true")
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
                
        try:
            df = reader.csv(file_path)
            self.logger.info(f"Successfully loaded {df.count()} records from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading CSV file {file_path}: {str(e)}")
            raise
            
    def load_parquet(self, file_path: str) -> DataFrame:
        """Load Parquet file"""
        self.logger.info(f"Loading Parquet file: {file_path}")
        
        try:
            df = self.spark.read.parquet(file_path)
            self.logger.info(f"Successfully loaded {df.count()} records from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading Parquet file {file_path}: {str(e)}")
            raise
            
    def load_json(self, file_path: str, multiline: bool = False) -> DataFrame:
        """Load JSON file"""
        self.logger.info(f"Loading JSON file: {file_path}")
        
        try:
            df = self.spark.read.option("multiline", multiline).json(file_path)
            self.logger.info(f"Successfully loaded {df.count()} records from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading JSON file {file_path}: {str(e)}")
            raise