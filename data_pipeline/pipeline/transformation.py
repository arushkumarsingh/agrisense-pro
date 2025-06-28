"""
Data transformation module for AgriSense Pro pipeline.
Handles data cleaning, derived fields, outlier detection, and calibration.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
from scipy import stats
from .config import PipelineConfig
from .utils import setup_logging, convert_timestamp_to_utc_plus_530, calculate_z_score


class DataTransformation:
    """Handles data transformation including cleaning, enrichment, and derived fields."""
    
    def __init__(self, config: PipelineConfig = None):
        """
        Initialize the data transformation component.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config or PipelineConfig()
        self.logger = setup_logging(__name__, self.config.LOG_LEVEL)
        
    def transform_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Apply complete transformation pipeline to the dataset.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (transformed_dataframe, transformation_stats)
        """
        transformation_stats = {
            'original_records': len(df),
            'records_after_cleaning': 0,
            'records_after_outlier_removal': 0,
            'records_final': 0,
            'duplicates_removed': 0,
            'missing_values_filled': 0,
            'outliers_detected': 0,
            'anomalous_readings': 0,
            'derived_fields_added': []
        }
        
        try:
            # Step 1: Data cleaning
            df_cleaned = self._clean_data(df, transformation_stats)
            
            # Step 2: Timestamp processing
            df_timestamped = self._process_timestamps(df_cleaned)
            
            # Step 3: Apply calibration
            df_calibrated = self._apply_calibration(df_timestamped)
            
            # Step 4: Outlier detection and handling
            df_no_outliers = self._handle_outliers(df_calibrated, transformation_stats)
            
            # Step 5: Add derived fields
            df_enriched = self._add_derived_fields(df_no_outliers, transformation_stats)
            
            # Step 6: Add anomaly detection
            df_final = self._add_anomaly_detection(df_enriched, transformation_stats)
            
            transformation_stats['records_final'] = len(df_final)
            
            self.logger.info(f"Transformation complete: {transformation_stats['records_final']} records processed")
            
            return df_final, transformation_stats
            
        except Exception as e:
            self.logger.error(f"Transformation error: {e}")
            transformation_stats['error'] = str(e)
            return df, transformation_stats
    
    def _clean_data(self, df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
        """
        Clean the dataset by removing duplicates and handling missing values.
        
        Args:
            df: Input DataFrame
            stats: Statistics dictionary to update
            
        Returns:
            Cleaned DataFrame
        """
        original_count = len(df)
        
        # Remove duplicates
        df_clean = df.drop_duplicates()
        stats['duplicates_removed'] = original_count - len(df_clean)
        
        # Handle missing values
        missing_counts = df_clean.isnull().sum()
        
        # Fill missing battery_level with default value
        if 'battery_level' in df_clean.columns:
            missing_battery = df_clean['battery_level'].isnull().sum()
            df_clean['battery_level'] = df_clean['battery_level'].fillna(100.0)
            stats['missing_values_filled'] += missing_battery
        
        # Drop rows with missing critical values
        critical_columns = ['sensor_id', 'timestamp', 'reading_type', 'value']
        df_clean = df_clean.dropna(subset=critical_columns)
        
        stats['records_after_cleaning'] = len(df_clean)
        
        self.logger.info(f"Data cleaning: removed {stats['duplicates_removed']} duplicates, "
                        f"filled {stats['missing_values_filled']} missing values")
        
        return df_clean
    
    def _process_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process timestamps to ISO 8601 format and convert to UTC+5:30.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with processed timestamps
        """
        df_processed = df.copy()
        
        # Convert timestamp to datetime if it's not already
        if 'timestamp' in df_processed.columns:
            df_processed['timestamp'] = pd.to_datetime(df_processed['timestamp'])
            
            # Convert to UTC+5:30
            df_processed['timestamp'] = df_processed['timestamp'].apply(
                convert_timestamp_to_utc_plus_530
            )
            
            # Add date column for partitioning
            df_processed['date'] = pd.to_datetime(df_processed['timestamp']).dt.date.astype(str)
            
            # Add hour column for time-based analysis
            df_processed['hour'] = pd.to_datetime(df_processed['timestamp']).dt.hour
        
        return df_processed
    
    def _apply_calibration(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply calibration parameters to sensor readings.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calibrated values
        """
        df_calibrated = df.copy()
        
        # Apply calibration for each reading type
        for reading_type in df_calibrated['reading_type'].unique():
            mask = df_calibrated['reading_type'] == reading_type
            calib_params = self.config.get_calibration_params(reading_type)
            
            if calib_params.multiplier != 1.0 or calib_params.offset != 0.0:
                df_calibrated.loc[mask, 'value'] = (
                    df_calibrated.loc[mask, 'value'] * calib_params.multiplier + calib_params.offset
                )
                
                # Add calibrated value column
                df_calibrated.loc[mask, 'calibrated_value'] = df_calibrated.loc[mask, 'value']
        
        return df_calibrated
    
    def _handle_outliers(self, df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
        """
        Detect and handle outliers using z-score method.
        
        Args:
            df: Input DataFrame
            stats: Statistics dictionary to update
            
        Returns:
            DataFrame with outliers handled
        """
        df_clean = df.copy()
        outliers_detected = 0
        
        # Detect outliers for each reading type
        for reading_type in df_clean['reading_type'].unique():
            mask = df_clean['reading_type'] == reading_type
            values = df_clean.loc[mask, 'value']
            
            if len(values) > 10:  # Need sufficient data for outlier detection
                # Calculate z-scores
                z_scores = np.abs(stats.zscore(values))
                outlier_mask = z_scores > self.config.Z_SCORE_THRESHOLD
                
                outliers_count = outlier_mask.sum()
                outliers_detected += outliers_count
                
                if outliers_count > 0:
                    # Replace outliers with median
                    median_value = values.median()
                    df_clean.loc[mask & outlier_mask, 'value'] = median_value
                    
                    # Mark as outlier
                    df_clean.loc[mask & outlier_mask, 'is_outlier'] = True
                else:
                    df_clean.loc[mask, 'is_outlier'] = False
            else:
                df_clean.loc[mask, 'is_outlier'] = False
        
        stats['outliers_detected'] = outliers_detected
        stats['records_after_outlier_removal'] = len(df_clean)
        
        self.logger.info(f"Outlier detection: {outliers_detected} outliers detected and handled")
        
        return df_clean
    
    def _add_derived_fields(self, df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
        """
        Add derived fields including daily averages and rolling averages.
        
        Args:
            df: Input DataFrame
            stats: Statistics dictionary to update
            
        Returns:
            DataFrame with derived fields
        """
        df_enriched = df.copy()
        derived_fields = []
        
        # Ensure timestamp is datetime
        df_enriched['timestamp'] = pd.to_datetime(df_enriched['timestamp'])
        
        # Add daily average per sensor and reading type
        daily_avg = df_enriched.groupby(['date', 'sensor_id', 'reading_type'])['value'].mean().reset_index()
        daily_avg = daily_avg.rename(columns={'value': 'daily_avg_value'})
        df_enriched = df_enriched.merge(daily_avg, on=['date', 'sensor_id', 'reading_type'], how='left')
        derived_fields.append('daily_avg_value')
        
        # Add 7-day rolling average (requires more data, so we'll calculate what we can)
        try:
            # Sort by timestamp for rolling calculations
            df_enriched = df_enriched.sort_values(['sensor_id', 'reading_type', 'timestamp'])
            
            # Calculate rolling average with minimum 2 days of data
            rolling_avg = df_enriched.groupby(['sensor_id', 'reading_type'])['value'].rolling(
                window=7, min_periods=2
            ).mean().reset_index()
            rolling_avg = rolling_avg.rename(columns={'value': 'rolling_7day_avg'})
            
            # Merge back
            df_enriched = df_enriched.merge(
                rolling_avg[['level_2', 'rolling_7day_avg']], 
                left_index=True, 
                right_on='level_2', 
                how='left'
            )
            df_enriched = df_enriched.drop('level_2', axis=1)
            derived_fields.append('rolling_7day_avg')
            
        except Exception as e:
            self.logger.warning(f"Could not calculate rolling average: {e}")
        
        # Add value change from previous reading
        df_enriched = df_enriched.sort_values(['sensor_id', 'reading_type', 'timestamp'])
        df_enriched['value_change'] = df_enriched.groupby(['sensor_id', 'reading_type'])['value'].diff()
        derived_fields.append('value_change')
        
        # Add time difference from previous reading
        df_enriched['time_diff_hours'] = df_enriched.groupby(['sensor_id', 'reading_type'])['timestamp'].diff().dt.total_seconds() / 3600
        derived_fields.append('time_diff_hours')
        
        stats['derived_fields_added'] = derived_fields
        
        self.logger.info(f"Added derived fields: {derived_fields}")
        
        return df_enriched
    
    def _add_anomaly_detection(self, df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
        """
        Add anomaly detection based on expected ranges.
        
        Args:
            df: Input DataFrame
            stats: Statistics dictionary to update
            
        Returns:
            DataFrame with anomaly detection
        """
        df_anomaly = df.copy()
        anomalous_count = 0
        
        # Check for anomalous readings based on expected ranges
        for reading_type in df_anomaly['reading_type'].unique():
            mask = df_anomaly['reading_type'] == reading_type
            expected_range = self.config.get_expected_range(reading_type)
            
            # Mark anomalous readings
            anomaly_mask = (
                (df_anomaly.loc[mask, 'value'] < expected_range.min_value) |
                (df_anomaly.loc[mask, 'value'] > expected_range.max_value)
            )
            
            df_anomaly.loc[mask, 'anomalous_reading'] = anomaly_mask
            anomalous_count += anomaly_mask.sum()
        
        # Also check for battery level anomalies
        if 'battery_level' in df_anomaly.columns:
            battery_range = self.config.get_expected_range('battery_level')
            battery_anomaly = (
                (df_anomaly['battery_level'] < battery_range.min_value) |
                (df_anomaly['battery_level'] > battery_range.max_value)
            )
            df_anomaly['battery_anomalous'] = battery_anomaly
            anomalous_count += battery_anomaly.sum()
        
        stats['anomalous_readings'] = anomalous_count
        
        self.logger.info(f"Anomaly detection: {anomalous_count} anomalous readings detected")
        
        return df_anomaly
    
    def get_transformation_summary(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate transformation summary.
        
        Args:
            stats: Transformation statistics
            
        Returns:
            Summary dictionary
        """
        summary = {
            'transformation_status': 'success' if 'error' not in stats else 'failed',
            'original_records': stats.get('original_records', 0),
            'final_records': stats.get('records_final', 0),
            'data_quality_improvements': {
                'duplicates_removed': stats.get('duplicates_removed', 0),
                'missing_values_filled': stats.get('missing_values_filled', 0),
                'outliers_handled': stats.get('outliers_detected', 0),
                'anomalous_readings': stats.get('anomalous_readings', 0)
            },
            'derived_fields': stats.get('derived_fields_added', []),
            'error': stats.get('error', None)
        }
        
        return summary


def main():
    """Main function for testing the transformation module."""
    import pandas as pd
    from .ingestion import DataIngestion
    
    # Create sample data for testing
    sample_data = pd.DataFrame({
        'sensor_id': ['sensor_001', 'sensor_001', 'sensor_002', 'sensor_002'],
        'timestamp': ['2023-06-01T10:00:00Z', '2023-06-01T11:00:00Z', 
                     '2023-06-01T10:00:00Z', '2023-06-01T11:00:00Z'],
        'reading_type': ['temperature', 'temperature', 'humidity', 'humidity'],
        'value': [25.5, 26.0, 60.0, 65.0],
        'battery_level': [95.0, 94.0, 90.0, 89.0]
    })
    
    # Initialize transformation
    transformer = DataTransformation()
    
    # Transform data
    transformed_df, stats = transformer.transform_data(sample_data)
    
    print("Transformation Summary:")
    print(f"Original records: {stats['original_records']}")
    print(f"Final records: {stats['records_final']}")
    print(f"Duplicates removed: {stats['duplicates_removed']}")
    print(f"Missing values filled: {stats['missing_values_filled']}")
    print(f"Outliers detected: {stats['outliers_detected']}")
    print(f"Anomalous readings: {stats['anomalous_readings']}")
    print(f"Derived fields: {stats['derived_fields_added']}")
    
    print("\nTransformed Data:")
    print(transformed_df.head())
    print(f"\nColumns: {list(transformed_df.columns)}")


if __name__ == "__main__":
    main()
