from pyspark.sql import SparkSession
import logging
from typing import Dict, Optional

def create_spark_session(app_name: str, config: Optional[Dict] = None) -> SparkSession:
    """Create and configure Spark session"""
    logger = logging.getLogger(__name__)
    logger.info(f"Creating Spark session: {app_name}")
    
    builder = SparkSession.builder.appName(app_name)
    
    # Default configurations
    default_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.statistics.histogram.enabled": "true"
    }
    
    # Merge with provided config
    if config:
        default_config.update(config)
    
    # Apply configurations
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created successfully")
    return spark

def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )