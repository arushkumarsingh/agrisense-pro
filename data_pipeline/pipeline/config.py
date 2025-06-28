"""
Configuration settings for the AgriSense Pro data pipeline.
"""
import os
from typing import Dict, Any, List
from dataclasses import dataclass


@dataclass
class CalibrationParams:
    """Calibration parameters for sensor readings."""
    multiplier: float = 1.0
    offset: float = 0.0
    unit: str = ""


@dataclass
class ExpectedRange:
    """Expected value ranges for different sensor types."""
    min_value: float
    max_value: float
    unit: str = ""


class PipelineConfig:
    """Configuration class for the data pipeline."""
    
    # Default paths
    RAW_DATA_PATH = os.environ.get('RAW_DATA_PATH', 'data/raw')
    PROCESSED_DATA_PATH = os.environ.get('PROCESSED_DATA_PATH', 'data/processed')
    QUALITY_REPORTS_PATH = os.environ.get('QUALITY_REPORTS_PATH', 'data/quality_reports')
    
    # File patterns
    RAW_FILE_PATTERN = "*.parquet"
    PROCESSED_FILE_PATTERN = "*.parquet"
    
    # Processing settings
    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10000'))
    MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '4'))
    
    # Validation settings
    Z_SCORE_THRESHOLD = float(os.environ.get('Z_SCORE_THRESHOLD', '3.0'))
    MISSING_VALUE_THRESHOLD = float(os.environ.get('MISSING_VALUE_THRESHOLD', '0.1'))
    
    # Timezone settings
    TARGET_TIMEZONE = os.environ.get('TARGET_TIMEZONE', 'Asia/Kolkata')  # UTC+5:30
    
    # Calibration parameters for different sensor types
    CALIBRATION_PARAMS = {
        'temperature': CalibrationParams(multiplier=1.0, offset=0.0, unit="°C"),
        'humidity': CalibrationParams(multiplier=1.0, offset=0.0, unit="%"),
        'soil_moisture': CalibrationParams(multiplier=1.0, offset=0.0, unit="%"),
        'light_intensity': CalibrationParams(multiplier=1.0, offset=0.0, unit="lux"),
        'air_pressure': CalibrationParams(multiplier=1.0, offset=0.0, unit="hPa"),
        'wind_speed': CalibrationParams(multiplier=1.0, offset=0.0, unit="m/s"),
        'rainfall': CalibrationParams(multiplier=1.0, offset=0.0, unit="mm"),
    }
    
    # Expected value ranges for anomaly detection
    EXPECTED_RANGES = {
        'temperature': ExpectedRange(min_value=-10.0, max_value=60.0, unit="°C"),
        'humidity': ExpectedRange(min_value=0.0, max_value=100.0, unit="%"),
        'soil_moisture': ExpectedRange(min_value=0.0, max_value=100.0, unit="%"),
        'light_intensity': ExpectedRange(min_value=0.0, max_value=100000.0, unit="lux"),
        'air_pressure': ExpectedRange(min_value=800.0, max_value=1200.0, unit="hPa"),
        'wind_speed': ExpectedRange(min_value=0.0, max_value=50.0, unit="m/s"),
        'rainfall': ExpectedRange(min_value=0.0, max_value=500.0, unit="mm"),
        'battery_level': ExpectedRange(min_value=0.0, max_value=100.0, unit="%"),
    }
    
    # Data quality thresholds
    QUALITY_THRESHOLDS = {
        'completeness_threshold': 0.95,  # 95% of data should be present
        'accuracy_threshold': 0.98,      # 98% of data should be within expected ranges
        'consistency_threshold': 0.90,   # 90% of data should be consistent
    }
    
    # DuckDB settings
    DUCKDB_SETTINGS = {
        'threads': MAX_WORKERS,
        'memory_limit': '2GB',
        'temp_directory': '/tmp/duckdb',
    }
    
    # Logging settings
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    @classmethod
    def get_calibration_params(cls, reading_type: str) -> CalibrationParams:
        """Get calibration parameters for a specific reading type."""
        return cls.CALIBRATION_PARAMS.get(reading_type, CalibrationParams())
    
    @classmethod
    def get_expected_range(cls, reading_type: str) -> ExpectedRange:
        """Get expected range for a specific reading type."""
        return cls.EXPECTED_RANGES.get(reading_type, ExpectedRange(0.0, 100.0))
    
    @classmethod
    def is_anomalous(cls, reading_type: str, value: float) -> bool:
        """Check if a value is anomalous for a given reading type."""
        expected_range = cls.get_expected_range(reading_type)
        return value < expected_range.min_value or value > expected_range.max_value
    
    @classmethod
    def apply_calibration(cls, reading_type: str, raw_value: float) -> float:
        """Apply calibration to a raw sensor value."""
        calib_params = cls.get_calibration_params(reading_type)
        return raw_value * calib_params.multiplier + calib_params.offset


# Default job configuration
DEFAULT_JOB_CONFIG = {
    'raw_data_path': PipelineConfig.RAW_DATA_PATH,
    'processed_data_path': PipelineConfig.PROCESSED_DATA_PATH,
    'quality_reports_path': PipelineConfig.QUALITY_REPORTS_PATH,
    'batch_size': PipelineConfig.BATCH_SIZE,
    'max_workers': PipelineConfig.MAX_WORKERS,
    'z_score_threshold': PipelineConfig.Z_SCORE_THRESHOLD,
    'missing_value_threshold': PipelineConfig.MISSING_VALUE_THRESHOLD,
    'target_timezone': PipelineConfig.TARGET_TIMEZONE,
    'enable_anomaly_detection': True,
    'enable_data_quality_validation': True,
    'enable_derived_fields': True,
    'compression': 'snappy',  # or 'gzip'
    'partition_by': ['date', 'sensor_id'],
}
