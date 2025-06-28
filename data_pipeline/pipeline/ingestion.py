"""
Data ingestion module for AgriSense Pro pipeline.
Handles reading Parquet files, schema validation, and incremental loading.
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import pandas as pd
import duckdb
from .config import PipelineConfig
from .utils import setup_logging, ensure_directory_exists, get_parquet_files, validate_file_path

logger = logging.getLogger(__name__)


class DataIngestion:
    """Handles data ingestion from Parquet files with DuckDB integration."""
    
    def __init__(self, config: PipelineConfig = None):
        """
        Initialize the data ingestion component.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config or PipelineConfig()
        self.duckdb_conn = None
        self._setup_duckdb()
        
    def _setup_duckdb(self):
        """Initialize DuckDB connection with optimized settings."""
        try:
            self.duckdb_conn = duckdb.connect(':memory:')
            
            # Apply DuckDB settings
            for setting, value in self.config.DUCKDB_SETTINGS.items():
                if setting == 'threads':
                    self.duckdb_conn.execute(f"SET threads={value}")
                elif setting == 'memory_limit':
                    self.duckdb_conn.execute(f"SET memory_limit='{value}'")
                    
            logger.info("DuckDB connection initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB: {e}")
            raise
    
    def get_files_to_process(self, start_date: Optional[str] = None, 
                           end_date: Optional[str] = None) -> List[str]:
        """
        Get list of files to process based on date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of file paths to process
        """
        all_files = get_parquet_files(self.config.RAW_DATA_PATH)
        
        if not start_date and not end_date:
            return all_files
        
        filtered_files = []
        for file_path in all_files:
            file_date = self._extract_date_from_filename(file_path)
            if file_date:
                if start_date and file_date < start_date:
                    continue
                if end_date and file_date > end_date:
                    continue
                filtered_files.append(file_path)
        
        logger.info(f"Found {len(filtered_files)} files to process")
        return filtered_files
    
    def _extract_date_from_filename(self, file_path: str) -> Optional[str]:
        """Extract date from filename (e.g., 2023-06-01.parquet -> 2023-06-01)."""
        filename = os.path.basename(file_path)
        if filename.endswith('.parquet'):
            date_part = filename.replace('.parquet', '')
            try:
                # Validate date format
                datetime.strptime(date_part, '%Y-%m-%d')
                return date_part
            except ValueError:
                return None
        return None
    
    def validate_file_schema(self, file_path: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate file schema using DuckDB.
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            Tuple of (is_valid, schema_info)
        """
        try:
            # Read schema using DuckDB
            schema_query = f"""
            DESCRIBE SELECT * FROM read_parquet('{file_path}')
            """
            schema_result = self.duckdb_conn.execute(schema_query).fetchall()
            
            # Expected schema
            expected_columns = {
                'sensor_id': 'VARCHAR',
                'timestamp': 'TIMESTAMP',
                'reading_type': 'VARCHAR', 
                'value': 'DOUBLE',
                'battery_level': 'DOUBLE'
            }
            
            # Validate schema
            actual_columns = {row[0]: row[1] for row in schema_result}
            missing_columns = set(expected_columns.keys()) - set(actual_columns.keys())
            
            schema_info = {
                'file_path': file_path,
                'actual_columns': actual_columns,
                'expected_columns': expected_columns,
                'missing_columns': list(missing_columns),
                'is_valid': len(missing_columns) == 0
            }
            
            if not schema_info['is_valid']:
                logger.warning(f"Schema validation failed for {file_path}: missing columns {missing_columns}")
            
            return schema_info['is_valid'], schema_info
            
        except Exception as e:
            logger.error(f"Schema validation error for {file_path}: {e}")
            return False, {'error': str(e), 'file_path': file_path}
    
    def read_file_with_validation(self, file_path: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """
        Read Parquet file with comprehensive validation.
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            Tuple of (dataframe, validation_stats)
        """
        validation_stats = {
            'file_path': file_path,
            'file_size_mb': 0.0,
            'records_total': 0,
            'records_valid': 0,
            'records_invalid': 0,
            'schema_valid': False,
            'errors': []
        }
        
        try:
            # Validate file exists and is readable
            if not validate_file_path(file_path):
                validation_stats['errors'].append("File not found or not readable")
                return None, validation_stats
            
            # Get file size
            validation_stats['file_size_mb'] = os.path.getsize(file_path) / (1024 * 1024)
            
            # Validate schema
            schema_valid, schema_info = self.validate_file_schema(file_path)
            validation_stats['schema_valid'] = schema_valid
            
            if not schema_valid:
                validation_stats['errors'].append(f"Schema validation failed: {schema_info}")
                return None, validation_stats
            
            # Read data using DuckDB for better performance
            query = f"""
            SELECT 
                sensor_id,
                timestamp,
                reading_type,
                value,
                battery_level
            FROM read_parquet('{file_path}')
            """
            
            df = self.duckdb_conn.execute(query).df()
            validation_stats['records_total'] = len(df)
            
            # Basic data validation
            if len(df) > 0:
                # Check for required columns
                required_columns = ['sensor_id', 'timestamp', 'reading_type', 'value']
                missing_cols = [col for col in required_columns if col not in df.columns]
                
                if missing_cols:
                    validation_stats['errors'].append(f"Missing required columns: {missing_cols}")
                    return None, validation_stats
                
                # Validate data types
                validation_stats.update(self._validate_data_types(df))
                
                # Count valid records
                validation_stats['records_valid'] = len(df.dropna(subset=['sensor_id', 'timestamp', 'reading_type', 'value']))
                validation_stats['records_invalid'] = validation_stats['records_total'] - validation_stats['records_valid']
                
                logger.info(f"Successfully read {validation_stats['records_valid']} valid records from {file_path}")
                return df, validation_stats
            else:
                validation_stats['errors'].append("File is empty")
                return None, validation_stats
                
        except Exception as e:
            error_msg = f"Error reading file {file_path}: {str(e)}"
            logger.error(error_msg)
            validation_stats['errors'].append(error_msg)
            return None, validation_stats
    
    def _validate_data_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate data types of columns."""
        type_validation = {
            'type_errors': [],
            'type_warnings': []
        }
        
        # Check sensor_id is string
        if 'sensor_id' in df.columns and not df['sensor_id'].dtype == 'object':
            type_validation['type_warnings'].append("sensor_id should be string type")
        
        # Check reading_type is string
        if 'reading_type' in df.columns and not df['reading_type'].dtype == 'object':
            type_validation['type_warnings'].append("reading_type should be string type")
        
        # Check value is numeric
        if 'value' in df.columns and not pd.api.types.is_numeric_dtype(df['value']):
            type_validation['type_errors'].append("value should be numeric type")
        
        # Check battery_level is numeric
        if 'battery_level' in df.columns and not pd.api.types.is_numeric_dtype(df['battery_level']):
            type_validation['type_warnings'].append("battery_level should be numeric type")
        
        return type_validation
    
    def get_ingestion_summary(self, processed_files: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate ingestion summary statistics.
        
        Args:
            processed_files: List of file processing results
            
        Returns:
            Summary statistics
        """
        summary = {
            'total_files': len(processed_files),
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'total_size_mb': 0.0,
            'errors': []
        }
        
        for file_result in processed_files:
            if file_result.get('success', False):
                summary['successful_files'] += 1
                summary['total_records'] += file_result.get('records_total', 0)
                summary['valid_records'] += file_result.get('records_valid', 0)
                summary['invalid_records'] += file_result.get('records_invalid', 0)
                summary['total_size_mb'] += file_result.get('file_size_mb', 0.0)
            else:
                summary['failed_files'] += 1
                summary['errors'].extend(file_result.get('errors', []))
        
        return summary
    
    def process_files(self, file_paths: List[str]) -> Tuple[List[pd.DataFrame], Dict[str, Any]]:
        """
        Process multiple files and return combined data.
        
        Args:
            file_paths: List of file paths to process
            
        Returns:
            Tuple of (list_of_dataframes, processing_summary)
        """
        dataframes = []
        processed_files = []
        
        for file_path in file_paths:
            logger.info(f"Processing file: {file_path}")
            
            df, validation_stats = self.read_file_with_validation(file_path)
            
            if df is not None:
                dataframes.append(df)
                validation_stats['success'] = True
            else:
                validation_stats['success'] = False
            
            processed_files.append(validation_stats)
        
        # Generate summary
        summary = self.get_ingestion_summary(processed_files)
        
        logger.info(f"Ingestion complete: {summary['successful_files']}/{summary['total_files']} files processed successfully")
        
        return dataframes, summary
    
    def close(self):
        """Close DuckDB connection."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
            logger.info("DuckDB connection closed")


def main():
    """Main function for testing the ingestion module."""
    ingestion = DataIngestion()
    
    try:
        # Get files to process
        files = ingestion.get_files_to_process()
        
        if not files:
            print("No files found to process")
            return
        
        # Process files
        dataframes, summary = ingestion.process_files(files)
        
        print("Ingestion Summary:")
        print(f"Total files: {summary['total_files']}")
        print(f"Successful: {summary['successful_files']}")
        print(f"Failed: {summary['failed_files']}")
        print(f"Total records: {summary['total_records']}")
        print(f"Valid records: {summary['valid_records']}")
        print(f"Invalid records: {summary['invalid_records']}")
        print(f"Total size: {summary['total_size_mb']:.2f} MB")
        
        if summary['errors']:
            print("Errors:")
            for error in summary['errors']:
                print(f"  - {error}")
        
        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            print(f"\nCombined dataset shape: {combined_df.shape}")
            print(f"Columns: {list(combined_df.columns)}")
            
    finally:
        ingestion.close()


if __name__ == "__main__":
    main()
