"""
Unit tests for the data ingestion module.
"""
import pytest
import pandas as pd
import tempfile
import os
from datetime import datetime
from unittest.mock import patch, MagicMock

from pipeline.ingestion import DataIngestion
from pipeline.config import PipelineConfig


class TestDataIngestion:
    """Test cases for DataIngestion class."""
    
    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return PipelineConfig()
    
    @pytest.fixture
    def ingestion(self, config):
        """Create a DataIngestion instance for testing."""
        return DataIngestion(config)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample sensor data."""
        return pd.DataFrame({
            'sensor_id': ['sensor_001', 'sensor_002', 'sensor_001'],
            'timestamp': ['2023-06-01T10:00:00Z', '2023-06-01T11:00:00Z', '2023-06-01T12:00:00Z'],
            'reading_type': ['temperature', 'humidity', 'temperature'],
            'value': [25.5, 60.0, 26.0],
            'battery_level': [95.0, 90.0, 94.0]
        })
    
    def test_init(self, ingestion):
        """Test initialization of DataIngestion."""
        assert ingestion.config is not None
        assert ingestion.logger is not None
        assert ingestion.duckdb_conn is not None
    
    def test_extract_date_from_filename(self, ingestion):
        """Test date extraction from filename."""
        # Valid date
        filename = "data/raw/2023-06-01.parquet"
        date = ingestion._extract_date_from_filename(filename)
        assert date == "2023-06-01"
        
        # Invalid date
        filename = "data/raw/invalid.parquet"
        date = ingestion._extract_date_from_filename(filename)
        assert date is None
        
        # No extension
        filename = "data/raw/2023-06-01"
        date = ingestion._extract_date_from_filename(filename)
        assert date is None
    
    def test_validate_file_schema(self, ingestion, sample_data):
        """Test file schema validation."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            sample_data.to_parquet(tmp_file.name, index=False)
            
            try:
                is_valid, schema_info = ingestion.validate_file_schema(tmp_file.name)
                
                assert is_valid is True
                assert 'actual_columns' in schema_info
                assert 'expected_columns' in schema_info
                assert len(schema_info['missing_columns']) == 0
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_validate_file_schema_missing_columns(self, ingestion):
        """Test schema validation with missing columns."""
        # Create data with missing columns
        incomplete_data = pd.DataFrame({
            'sensor_id': ['sensor_001'],
            'timestamp': ['2023-06-01T10:00:00Z'],
            'value': [25.5]
            # Missing reading_type and battery_level
        })
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            incomplete_data.to_parquet(tmp_file.name, index=False)
            
            try:
                is_valid, schema_info = ingestion.validate_file_schema(tmp_file.name)
                
                assert is_valid is False
                assert 'reading_type' in schema_info['missing_columns']
                assert 'battery_level' in schema_info['missing_columns']
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_read_file_with_validation_success(self, ingestion, sample_data):
        """Test successful file reading with validation."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            sample_data.to_parquet(tmp_file.name, index=False)
            
            try:
                df, validation_stats = ingestion.read_file_with_validation(tmp_file.name)
                
                assert df is not None
                assert len(df) == 3
                assert validation_stats['success'] is True
                assert validation_stats['records_total'] == 3
                assert validation_stats['records_valid'] == 3
                assert validation_stats['schema_valid'] is True
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_read_file_with_validation_file_not_found(self, ingestion):
        """Test file reading with non-existent file."""
        df, validation_stats = ingestion.read_file_with_validation("nonexistent.parquet")
        
        assert df is None
        assert validation_stats['success'] is False
        assert "File not found" in validation_stats['errors'][0]
    
    def test_read_file_with_validation_empty_file(self, ingestion):
        """Test file reading with empty file."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            # Create empty DataFrame
            empty_df = pd.DataFrame()
            empty_df.to_parquet(tmp_file.name, index=False)
            
            try:
                df, validation_stats = ingestion.read_file_with_validation(tmp_file.name)
                
                assert df is None
                assert validation_stats['success'] is False
                assert "File is empty" in validation_stats['errors'][0]
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_validate_data_types(self, ingestion):
        """Test data type validation."""
        # Valid data types
        valid_df = pd.DataFrame({
            'sensor_id': ['sensor_001'],
            'timestamp': ['2023-06-01T10:00:00Z'],
            'reading_type': ['temperature'],
            'value': [25.5],
            'battery_level': [95.0]
        })
        
        type_validation = ingestion._validate_data_types(valid_df)
        assert len(type_validation['type_errors']) == 0
        
        # Invalid data types
        invalid_df = pd.DataFrame({
            'sensor_id': [123],  # Should be string
            'timestamp': ['2023-06-01T10:00:00Z'],
            'reading_type': ['temperature'],
            'value': ['not_numeric'],  # Should be numeric
            'battery_level': [95.0]
        })
        
        type_validation = ingestion._validate_data_types(invalid_df)
        assert len(type_validation['type_errors']) > 0
        assert "value should be numeric type" in type_validation['type_errors'][0]
    
    def test_get_ingestion_summary(self, ingestion):
        """Test ingestion summary generation."""
        processed_files = [
            {
                'success': True,
                'records_total': 100,
                'records_valid': 95,
                'records_invalid': 5,
                'file_size_mb': 1.5
            },
            {
                'success': False,
                'records_total': 50,
                'records_valid': 0,
                'records_invalid': 50,
                'file_size_mb': 0.8,
                'errors': ['Schema validation failed']
            }
        ]
        
        summary = ingestion.get_ingestion_summary(processed_files)
        
        assert summary['total_files'] == 2
        assert summary['successful_files'] == 1
        assert summary['failed_files'] == 1
        assert summary['total_records'] == 100
        assert summary['valid_records'] == 95
        assert summary['invalid_records'] == 5
        assert summary['total_size_mb'] == 1.5
        assert len(summary['errors']) == 1
    
    @patch('pipeline.ingestion.get_parquet_files')
    def test_get_files_to_process_no_date_filter(self, mock_get_files, ingestion):
        """Test getting files to process without date filter."""
        mock_get_files.return_value = [
            'data/raw/2023-06-01.parquet',
            'data/raw/2023-06-02.parquet'
        ]
        
        files = ingestion.get_files_to_process()
        
        assert len(files) == 2
        mock_get_files.assert_called_once()
    
    @patch('pipeline.ingestion.get_parquet_files')
    def test_get_files_to_process_with_date_filter(self, mock_get_files, ingestion):
        """Test getting files to process with date filter."""
        mock_get_files.return_value = [
            'data/raw/2023-06-01.parquet',
            'data/raw/2023-06-02.parquet',
            'data/raw/2023-06-03.parquet'
        ]
        
        files = ingestion.get_files_to_process(start_date='2023-06-02', end_date='2023-06-02')
        
        assert len(files) == 1
        assert '2023-06-02.parquet' in files[0]
    
    def test_process_files(self, ingestion, sample_data):
        """Test processing multiple files."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file1, \
             tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file2:
            
            # Create two test files
            sample_data.to_parquet(tmp_file1.name, index=False)
            sample_data.to_parquet(tmp_file2.name, index=False)
            
            try:
                dataframes, summary = ingestion.process_files([tmp_file1.name, tmp_file2.name])
                
                assert len(dataframes) == 2
                assert summary['total_files'] == 2
                assert summary['successful_files'] == 2
                assert summary['failed_files'] == 0
                assert summary['total_records'] == 6  # 3 records per file
                
            finally:
                os.unlink(tmp_file1.name)
                os.unlink(tmp_file2.name)
    
    def test_cleanup(self, ingestion):
        """Test cleanup method."""
        # This should not raise any exceptions
        ingestion.close()
        
        # Verify connection is closed
        assert ingestion.duckdb_conn is None or ingestion.duckdb_conn.closed


if __name__ == "__main__":
    pytest.main([__file__])
