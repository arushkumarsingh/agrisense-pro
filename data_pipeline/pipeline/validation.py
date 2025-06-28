"""
Data validation module for AgriSense Pro pipeline.
Uses DuckDB for comprehensive data quality validation and reporting.
"""
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import duckdb
from .config import PipelineConfig
from .utils import setup_logging, ensure_directory_exists, save_json_report


class DataValidation:
    """Handles data quality validation using DuckDB queries."""
    
    def __init__(self, config: PipelineConfig = None):
        """
        Initialize the data validation component.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config or PipelineConfig()
        self.logger = setup_logging(__name__, self.config.LOG_LEVEL)
        self.duckdb_conn = None
        self._setup_duckdb()
        
    def _setup_duckdb(self):
        """Initialize DuckDB connection for validation queries."""
        try:
            self.duckdb_conn = duckdb.connect(':memory:')
            
            # Apply DuckDB settings
            for setting, value in self.config.DUCKDB_SETTINGS.items():
                if setting == 'threads':
                    self.duckdb_conn.execute(f"SET threads={value}")
                elif setting == 'memory_limit':
                    self.duckdb_conn.execute(f"SET memory_limit='{value}'")
                    
            self.logger.info("DuckDB validation connection initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize DuckDB for validation: {e}")
            raise
    
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive data quality validation.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validation results dictionary
        """
        validation_results = {
            'validation_timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'data_types': {},
            'value_ranges': {},
            'missing_values': {},
            'anomalies': {},
            'time_coverage': {},
            'data_gaps': {},
            'quality_score': 0.0,
            'issues': []
        }
        
        try:
            # Register DataFrame in DuckDB
            self.duckdb_conn.register('sensor_data', df)
            
            # Perform validation checks
            validation_results.update(self._validate_data_types())
            validation_results.update(self._validate_value_ranges())
            validation_results.update(self._validate_missing_values())
            validation_results.update(self._validate_anomalies())
            validation_results.update(self._validate_time_coverage())
            validation_results.update(self._detect_data_gaps())
            
            # Calculate overall quality score
            validation_results['quality_score'] = self._calculate_quality_score(validation_results)
            
            self.logger.info(f"Data quality validation complete. Score: {validation_results['quality_score']:.2f}")
            
        except Exception as e:
            error_msg = f"Validation error: {str(e)}"
            self.logger.error(error_msg)
            validation_results['issues'].append(error_msg)
            validation_results['quality_score'] = 0.0
            
        return validation_results
    
    def _validate_data_types(self) -> Dict[str, Any]:
        """Validate data types of columns."""
        type_validation = {'data_types': {}}
        
        try:
            # Get column types using DuckDB
            schema_query = "DESCRIBE sensor_data"
            schema_result = self.duckdb_conn.execute(schema_query).fetchall()
            
            for column_name, column_type, _, _, _, _ in schema_result:
                type_validation['data_types'][column_name] = {
                    'actual_type': column_type,
                    'is_valid': self._is_valid_data_type(column_name, column_type)
                }
                
        except Exception as e:
            self.logger.error(f"Data type validation error: {e}")
            
        return type_validation
    
    def _is_valid_data_type(self, column_name: str, column_type: str) -> bool:
        """Check if data type is valid for the column."""
        expected_types = {
            'sensor_id': ['VARCHAR', 'STRING'],
            'timestamp': ['TIMESTAMP', 'DATETIME'],
            'reading_type': ['VARCHAR', 'STRING'],
            'value': ['DOUBLE', 'FLOAT', 'DECIMAL'],
            'battery_level': ['DOUBLE', 'FLOAT', 'DECIMAL']
        }
        
        if column_name in expected_types:
            return any(expected_type in column_type.upper() for expected_type in expected_types[column_name])
        
        return True  # Unknown columns are considered valid
    
    def _validate_value_ranges(self) -> Dict[str, Any]:
        """Validate value ranges for different reading types."""
        range_validation = {'value_ranges': {}}
        
        try:
            # Get unique reading types
            reading_types_query = "SELECT DISTINCT reading_type FROM sensor_data WHERE reading_type IS NOT NULL"
            reading_types = self.duckdb_conn.execute(reading_types_query).fetchall()
            
            for (reading_type,) in reading_types:
                expected_range = self.config.get_expected_range(reading_type)
                
                # Check values within expected range
                range_query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN value >= {expected_range.min_value} AND value <= {expected_range.max_value} THEN 1 END) as valid_count,
                    MIN(value) as min_value,
                    MAX(value) as max_value,
                    AVG(value) as avg_value
                FROM sensor_data 
                WHERE reading_type = '{reading_type}'
                """
                
                result = self.duckdb_conn.execute(range_query).fetchone()
                
                if result:
                    total_count, valid_count, min_val, max_val, avg_val = result
                    
                    range_validation['value_ranges'][reading_type] = {
                        'expected_min': expected_range.min_value,
                        'expected_max': expected_range.max_value,
                        'actual_min': min_val,
                        'actual_max': max_val,
                        'actual_avg': avg_val,
                        'total_count': total_count,
                        'valid_count': valid_count,
                        'valid_percentage': (valid_count / total_count * 100) if total_count > 0 else 0,
                        'out_of_range_count': total_count - valid_count
                    }
                    
        except Exception as e:
            self.logger.error(f"Value range validation error: {e}")
            
        return range_validation
    
    def _validate_missing_values(self) -> Dict[str, Any]:
        """Validate missing values in the dataset."""
        missing_validation = {'missing_values': {}}
        
        try:
            # Check missing values for each column
            columns_query = "DESCRIBE sensor_data"
            columns = self.duckdb_conn.execute(columns_query).fetchall()
            
            for column_name, _, _, _, _, _ in columns:
                missing_query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN {column_name} IS NULL THEN 1 END) as missing_count
                FROM sensor_data
                """
                
                result = self.duckdb_conn.execute(missing_query).fetchone()
                
                if result:
                    total_count, missing_count = result
                    missing_percentage = (missing_count / total_count * 100) if total_count > 0 else 0
                    
                    missing_validation['missing_values'][column_name] = {
                        'total_count': total_count,
                        'missing_count': missing_count,
                        'missing_percentage': missing_percentage,
                        'is_acceptable': missing_percentage <= (self.config.MISSING_VALUE_THRESHOLD * 100)
                    }
                    
        except Exception as e:
            self.logger.error(f"Missing value validation error: {e}")
            
        return missing_validation
    
    def _validate_anomalies(self) -> Dict[str, Any]:
        """Validate anomalous readings."""
        anomaly_validation = {'anomalies': {}}
        
        try:
            # Check for anomalous readings
            anomaly_query = """
            SELECT 
                reading_type,
                COUNT(*) as total_count,
                COUNT(CASE WHEN anomalous_reading = true THEN 1 END) as anomalous_count
            FROM sensor_data 
            WHERE reading_type IS NOT NULL
            GROUP BY reading_type
            """
            
            results = self.duckdb_conn.execute(anomaly_query).fetchall()
            
            for reading_type, total_count, anomalous_count in results:
                anomalous_percentage = (anomalous_count / total_count * 100) if total_count > 0 else 0
                
                anomaly_validation['anomalies'][reading_type] = {
                    'total_count': total_count,
                    'anomalous_count': anomalous_count,
                    'anomalous_percentage': anomalous_percentage
                }
                
        except Exception as e:
            self.logger.error(f"Anomaly validation error: {e}")
            
        return anomaly_validation
    
    def _validate_time_coverage(self) -> Dict[str, Any]:
        """Validate time coverage and consistency."""
        time_validation = {'time_coverage': {}}
        
        try:
            # Check time coverage per sensor
            time_coverage_query = """
            SELECT 
                sensor_id,
                reading_type,
                COUNT(*) as reading_count,
                MIN(timestamp) as first_reading,
                MAX(timestamp) as last_reading,
                COUNT(DISTINCT DATE(timestamp)) as days_with_data
            FROM sensor_data 
            WHERE sensor_id IS NOT NULL AND reading_type IS NOT NULL
            GROUP BY sensor_id, reading_type
            """
            
            results = self.duckdb_conn.execute(time_coverage_query).fetchall()
            
            for sensor_id, reading_type, reading_count, first_reading, last_reading, days_with_data in results:
                key = f"{sensor_id}_{reading_type}"
                
                # Calculate time span
                if first_reading and last_reading:
                    time_span_days = (last_reading - first_reading).days + 1
                    coverage_percentage = (days_with_data / time_span_days * 100) if time_span_days > 0 else 0
                else:
                    time_span_days = 0
                    coverage_percentage = 0
                
                time_validation['time_coverage'][key] = {
                    'sensor_id': sensor_id,
                    'reading_type': reading_type,
                    'reading_count': reading_count,
                    'first_reading': first_reading.isoformat() if first_reading else None,
                    'last_reading': last_reading.isoformat() if last_reading else None,
                    'time_span_days': time_span_days,
                    'days_with_data': days_with_data,
                    'coverage_percentage': coverage_percentage
                }
                
        except Exception as e:
            self.logger.error(f"Time coverage validation error: {e}")
            
        return time_validation
    
    def _detect_data_gaps(self) -> Dict[str, Any]:
        """Detect gaps in hourly data using DuckDB generate_series."""
        gap_validation = {'data_gaps': {}}
        
        try:
            # For each sensor and reading type, check for hourly gaps
            sensors_query = """
            SELECT DISTINCT sensor_id, reading_type 
            FROM sensor_data 
            WHERE sensor_id IS NOT NULL AND reading_type IS NOT NULL
            """
            
            sensors = self.duckdb_conn.execute(sensors_query).fetchall()
            
            for sensor_id, reading_type in sensors:
                # Get the date range for this sensor/reading_type
                date_range_query = f"""
                SELECT 
                    MIN(DATE(timestamp)) as start_date,
                    MAX(DATE(timestamp)) as end_date
                FROM sensor_data 
                WHERE sensor_id = '{sensor_id}' AND reading_type = '{reading_type}'
                """
                
                date_result = self.duckdb_conn.execute(date_range_query).fetchone()
                
                if date_result and date_result[0] and date_result[1]:
                    start_date, end_date = date_result
                    
                    # Generate expected hourly timestamps
                    expected_hours_query = f"""
                    SELECT 
                        generate_series(
                            '{start_date} 00:00:00'::timestamp,
                            '{end_date} 23:00:00'::timestamp,
                            INTERVAL '1 hour'
                        ) as expected_hour
                    """
                    
                    # Check which expected hours are missing
                    gap_query = f"""
                    WITH expected_hours AS (
                        {expected_hours_query}
                    ),
                    actual_readings AS (
                        SELECT DISTINCT DATE_TRUNC('hour', timestamp) as actual_hour
                        FROM sensor_data 
                        WHERE sensor_id = '{sensor_id}' AND reading_type = '{reading_type}'
                    )
                    SELECT 
                        expected_hour,
                        CASE WHEN actual_hour IS NULL THEN 'MISSING' ELSE 'PRESENT' END as status
                    FROM expected_hours
                    LEFT JOIN actual_readings ON expected_hour = actual_hour
                    WHERE actual_hour IS NULL
                    ORDER BY expected_hour
                    """
                    
                    gap_results = self.duckdb_conn.execute(gap_query).fetchall()
                    
                    key = f"{sensor_id}_{reading_type}"
                    gap_validation['data_gaps'][key] = {
                        'sensor_id': sensor_id,
                        'reading_type': reading_type,
                        'start_date': start_date.isoformat(),
                        'end_date': end_date.isoformat(),
                        'missing_hours': [gap[0].isoformat() for gap in gap_results],
                        'missing_hours_count': len(gap_results)
                    }
                    
        except Exception as e:
            self.logger.error(f"Data gap detection error: {e}")
            
        return gap_validation
    
    def _calculate_quality_score(self, validation_results: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        score = 100.0
        deductions = 0.0
        
        # Deduct for missing values
        missing_values = validation_results.get('missing_values', {})
        for column, stats in missing_values.items():
            if not stats.get('is_acceptable', True):
                deductions += 10.0  # 10 points deduction for unacceptable missing values
        
        # Deduct for out-of-range values
        value_ranges = validation_results.get('value_ranges', {})
        for reading_type, stats in value_ranges.items():
            valid_percentage = stats.get('valid_percentage', 100.0)
            if valid_percentage < 95.0:  # Less than 95% valid
                deductions += (100.0 - valid_percentage) * 0.5  # 0.5 points per percentage point
        
        # Deduct for anomalies
        anomalies = validation_results.get('anomalies', {})
        for reading_type, stats in anomalies.items():
            anomalous_percentage = stats.get('anomalous_percentage', 0.0)
            if anomalous_percentage > 5.0:  # More than 5% anomalous
                deductions += anomalous_percentage * 0.3  # 0.3 points per percentage point
        
        # Deduct for data gaps
        data_gaps = validation_results.get('data_gaps', {})
        total_missing_hours = sum(stats.get('missing_hours_count', 0) for stats in data_gaps.values())
        if total_missing_hours > 0:
            deductions += min(total_missing_hours * 0.1, 20.0)  # Max 20 points deduction for gaps
        
        final_score = max(0.0, score - deductions)
        return round(final_score, 2)
    
    def generate_quality_report(self, validation_results: Dict[str, Any], 
                              output_path: Optional[str] = None) -> str:
        """
        Generate comprehensive data quality report.
        
        Args:
            validation_results: Validation results from validate_data_quality
            output_path: Optional output path for the report
            
        Returns:
            Path to the generated report
        """
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(
                self.config.QUALITY_REPORTS_PATH,
                f'data_quality_report_{timestamp}.json'
            )
        
        ensure_directory_exists(os.path.dirname(output_path))
        
        # Save detailed JSON report
        save_json_report(validation_results, output_path)
        
        # Generate CSV summary report
        csv_path = output_path.replace('.json', '.csv')
        self._generate_csv_summary(validation_results, csv_path)
        
        self.logger.info(f"Quality report generated: {output_path}")
        return output_path
    
    def _generate_csv_summary(self, validation_results: Dict[str, Any], csv_path: str):
        """Generate CSV summary of validation results."""
        summary_rows = []
        
        # Overall summary
        summary_rows.append({
            'metric': 'Overall Quality Score',
            'value': validation_results.get('quality_score', 0.0),
            'category': 'Overall'
        })
        
        summary_rows.append({
            'metric': 'Total Records',
            'value': validation_results.get('total_records', 0),
            'category': 'Overall'
        })
        
        # Missing values summary
        missing_values = validation_results.get('missing_values', {})
        for column, stats in missing_values.items():
            summary_rows.append({
                'metric': f'Missing Values - {column}',
                'value': f"{stats.get('missing_percentage', 0.0):.2f}%",
                'category': 'Data Completeness'
            })
        
        # Value ranges summary
        value_ranges = validation_results.get('value_ranges', {})
        for reading_type, stats in value_ranges.items():
            summary_rows.append({
                'metric': f'Valid Range - {reading_type}',
                'value': f"{stats.get('valid_percentage', 0.0):.2f}%",
                'category': 'Data Accuracy'
            })
        
        # Anomalies summary
        anomalies = validation_results.get('anomalies', {})
        for reading_type, stats in anomalies.items():
            summary_rows.append({
                'metric': f'Anomalous Readings - {reading_type}',
                'value': f"{stats.get('anomalous_percentage', 0.0):.2f}%",
                'category': 'Data Quality'
            })
        
        # Data gaps summary
        data_gaps = validation_results.get('data_gaps', {})
        total_missing_hours = sum(stats.get('missing_hours_count', 0) for stats in data_gaps.values())
        summary_rows.append({
            'metric': 'Total Missing Hours',
            'value': total_missing_hours,
            'category': 'Data Continuity'
        })
        
        # Create DataFrame and save to CSV
        summary_df = pd.DataFrame(summary_rows)
        summary_df.to_csv(csv_path, index=False)
        
        self.logger.info(f"CSV summary generated: {csv_path}")
    
    def close(self):
        """Close DuckDB connection."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
            self.logger.info("DuckDB validation connection closed")


def main():
    """Main function for testing the validation module."""
    import pandas as pd
    from .ingestion import DataIngestion
    from .transformation import DataTransformation
    
    # Create sample data for testing
    sample_data = pd.DataFrame({
        'sensor_id': ['sensor_001', 'sensor_001', 'sensor_002', 'sensor_002'],
        'timestamp': ['2023-06-01T10:00:00Z', '2023-06-01T11:00:00Z', 
                     '2023-06-01T10:00:00Z', '2023-06-01T11:00:00Z'],
        'reading_type': ['temperature', 'temperature', 'humidity', 'humidity'],
        'value': [25.5, 26.0, 60.0, 65.0],
        'battery_level': [95.0, 94.0, 90.0, 89.0]
    })
    
    # Transform data first to get all required columns
    transformer = DataTransformation()
    transformed_df, _ = transformer.transform_data(sample_data)
    
    # Initialize validation
    validator = DataValidation()
    
    try:
        # Validate data
        validation_results = validator.validate_data_quality(transformed_df)
        
        # Generate report
        report_path = validator.generate_quality_report(validation_results)
        
        print("Validation Results:")
        print(f"Quality Score: {validation_results['quality_score']}")
        print(f"Total Records: {validation_results['total_records']}")
        print(f"Report saved to: {report_path}")
        
        # Print some key metrics
        print("\nKey Metrics:")
        for metric, value in validation_results.get('missing_values', {}).items():
            print(f"Missing {metric}: {value['missing_percentage']:.2f}%")
        
    finally:
        validator.close()


if __name__ == "__main__":
    main()
