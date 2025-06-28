"""
Utility functions for the AgriSense Pro data pipeline.
"""
import logging
import os
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
import pytz
import pandas as pd
from pathlib import Path


def setup_logging(name: str, level: str = 'INFO', log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up logging configuration.
    
    Args:
        name: Logger name
        level: Logging level
        log_file: Optional log file path
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def convert_timestamp_to_utc_plus_530(timestamp: Union[str, datetime, pd.Timestamp]) -> str:
    """
    Convert timestamp to UTC+5:30 (IST) format.
    
    Args:
        timestamp: Input timestamp
        
    Returns:
        ISO 8601 formatted timestamp string in UTC+5:30
    """
    if isinstance(timestamp, str):
        # Try to parse the timestamp
        try:
            dt = pd.to_datetime(timestamp)
        except:
            # If parsing fails, assume it's already in the correct format
            return timestamp
    elif isinstance(timestamp, pd.Timestamp):
        dt = timestamp.to_pydatetime()
    else:
        dt = timestamp
    
    # Convert to UTC+5:30
    ist_tz = pytz.timezone('Asia/Kolkata')
    
    # If timestamp is timezone-naive, assume it's in UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    # Convert to IST
    ist_dt = dt.astimezone(ist_tz)
    
    return ist_dt.isoformat()


def ensure_directory_exists(path: str) -> None:
    """
    Ensure a directory exists, create if it doesn't.
    
    Args:
        path: Directory path
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File size in MB
    """
    try:
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 * 1024)
    except OSError:
        return 0.0


def format_duration(seconds: float) -> str:
    """
    Format duration in a human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def calculate_z_score(value: float, mean: float, std: float) -> float:
    """
    Calculate z-score for outlier detection.
    
    Args:
        value: Value to calculate z-score for
        mean: Mean of the distribution
        std: Standard deviation of the distribution
        
    Returns:
        Z-score
    """
    if std == 0:
        return 0.0
    return abs((value - mean) / std)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if division by zero
        
    Returns:
        Result of division or default value
    """
    try:
        return numerator / denominator if denominator != 0 else default
    except (TypeError, ZeroDivisionError):
        return default


def save_json_report(data: Dict[str, Any], file_path: str) -> None:
    """
    Save data as JSON report.
    
    Args:
        data: Data to save
        file_path: Output file path
    """
    ensure_directory_exists(os.path.dirname(file_path))
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)


def load_json_config(file_path: str) -> Dict[str, Any]:
    """
    Load JSON configuration file.
    
    Args:
        file_path: Path to JSON config file
        
    Returns:
        Configuration dictionary
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file {file_path}: {e}")


def get_parquet_files(directory: str, pattern: str = "*.parquet") -> List[str]:
    """
    Get list of Parquet files in a directory.
    
    Args:
        directory: Directory to search
        pattern: File pattern to match
        
    Returns:
        List of file paths
    """
    if not os.path.exists(directory):
        return []
    
    files = []
    for file in os.listdir(directory):
        if file.endswith('.parquet'):
            files.append(os.path.join(directory, file))
    
    return sorted(files)


def validate_file_path(file_path: str) -> bool:
    """
    Validate if a file path exists and is readable.
    
    Args:
        file_path: Path to validate
        
    Returns:
        True if file exists and is readable
    """
    return os.path.isfile(file_path) and os.access(file_path, os.R_OK)


def get_memory_usage_mb() -> float:
    """
    Get current memory usage in MB.
    
    Returns:
        Memory usage in MB
    """
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)
    except ImportError:
        return 0.0


def create_partition_path(base_path: str, partition_values: Dict[str, str]) -> str:
    """
    Create partition path for Parquet files.
    
    Args:
        base_path: Base directory path
        partition_values: Dictionary of partition column names and values
        
    Returns:
        Partitioned path
    """
    path_parts = [base_path]
    for col, value in partition_values.items():
        path_parts.append(f"{col}={value}")
    
    return os.path.join(*path_parts)


def parse_partition_path(file_path: str) -> Dict[str, str]:
    """
    Parse partition information from file path.
    
    Args:
        file_path: File path with partitions
        
    Returns:
        Dictionary of partition column names and values
    """
    partitions = {}
    path_parts = file_path.split(os.sep)
    
    for part in path_parts:
        if '=' in part:
            col, value = part.split('=', 1)
            partitions[col] = value
    
    return partitions


def calculate_statistics(df: pd.DataFrame, numeric_columns: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Calculate basic statistics for numeric columns.
    
    Args:
        df: DataFrame to analyze
        numeric_columns: List of numeric column names
        
    Returns:
        Dictionary of statistics for each column
    """
    stats = {}
    
    for col in numeric_columns:
        if col in df.columns:
            col_stats = df[col].describe()
            stats[col] = {
                'count': col_stats.get('count', 0),
                'mean': col_stats.get('mean', 0.0),
                'std': col_stats.get('std', 0.0),
                'min': col_stats.get('min', 0.0),
                'max': col_stats.get('max', 0.0),
                'median': df[col].median(),
                'missing_count': df[col].isnull().sum(),
                'missing_pct': (df[col].isnull().sum() / len(df)) * 100
            }
    
    return stats


def log_pipeline_summary(logger: logging.Logger, stats: Dict[str, Any]) -> None:
    """
    Log pipeline execution summary.
    
    Args:
        logger: Logger instance
        stats: Pipeline statistics
    """
    logger.info("=" * 60)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    
    if 'files_processed' in stats:
        logger.info(f"Files processed: {stats['files_processed']}")
    
    if 'records_processed' in stats:
        logger.info(f"Records processed: {stats['records_processed']:,}")
    
    if 'records_failed' in stats:
        logger.info(f"Records failed: {stats['records_failed']:,}")
    
    if 'execution_time' in stats:
        logger.info(f"Execution time: {format_duration(stats['execution_time'])}")
    
    if 'memory_usage' in stats:
        logger.info(f"Memory usage: {stats['memory_usage']:.1f} MB")
    
    if 'quality_score' in stats:
        logger.info(f"Data quality score: {stats['quality_score']:.2f}%")
    
    logger.info("=" * 60)
