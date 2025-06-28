"""
Data loading module for AgriSense Pro pipeline.
Handles storing transformed data in optimized Parquet format with partitioning.
"""
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from pathlib import Path
from .config import PipelineConfig
from .utils import setup_logging, ensure_directory_exists, get_memory_usage_mb


class DataLoading:
    """Handles data loading and storage in optimized Parquet format."""
    
    def __init__(self, config: PipelineConfig = None):
        """
        Initialize the data loading component.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config or PipelineConfig()
        self.logger = setup_logging(__name__, self.config.LOG_LEVEL)
        
    def load_data(self, df: pd.DataFrame, output_path: Optional[str] = None,
                 partition_by: Optional[List[str]] = None, 
                 compression: Optional[str] = None) -> Dict[str, Any]:
        """
        Load transformed data to storage with optimization.
        
        Args:
            df: Transformed DataFrame to load
            output_path: Output path for the data
            partition_by: Columns to partition by
            compression: Compression type (snappy, gzip, etc.)
            
        Returns:
            Loading statistics dictionary
        """
        loading_stats = {
            'records_loaded': 0,
            'output_path': '',
            'file_size_mb': 0.0,
            'partition_count': 0,
            'compression_ratio': 0.0,
            'loading_time_seconds': 0.0,
            'memory_usage_mb': 0.0,
            'success': False,
            'error': None
        }
        
        try:
            start_time = datetime.now()
            initial_memory = get_memory_usage_mb()
            
            # Set default values
            if output_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = os.path.join(
                    self.config.PROCESSED_DATA_PATH,
                    f'processed_data_{timestamp}.parquet'
                )
            
            if partition_by is None:
                partition_by = self.config.DEFAULT_JOB_CONFIG.get('partition_by', ['date'])
            
            if compression is None:
                compression = self.config.DEFAULT_JOB_CONFIG.get('compression', 'snappy')
            
            # Ensure output directory exists
            ensure_directory_exists(os.path.dirname(output_path))
            
            # Prepare data for loading
            df_optimized = self._optimize_dataframe(df)
            
            # Load data with partitioning if specified
            if partition_by and all(col in df_optimized.columns for col in partition_by):
                loading_stats.update(
                    self._load_partitioned_data(df_optimized, output_path, partition_by, compression)
                )
            else:
                loading_stats.update(
                    self._load_single_file(df_optimized, output_path, compression)
                )
            
            # Calculate final statistics
            end_time = datetime.now()
            loading_stats['loading_time_seconds'] = (end_time - start_time).total_seconds()
            loading_stats['memory_usage_mb'] = get_memory_usage_mb() - initial_memory
            loading_stats['success'] = True
            loading_stats['output_path'] = output_path
            
            self.logger.info(f"Data loading complete: {loading_stats['records_loaded']} records loaded to {output_path}")
            
        except Exception as e:
            error_msg = f"Data loading error: {str(e)}"
            self.logger.error(error_msg)
            loading_stats['error'] = error_msg
            loading_stats['success'] = False
            
        return loading_stats
    
    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame for storage efficiency.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Optimized DataFrame
        """
        df_optimized = df.copy()
        
        # Optimize data types for storage
        for column in df_optimized.columns:
            if df_optimized[column].dtype == 'object':
                # Check if it's a string column with limited unique values
                unique_ratio = df_optimized[column].nunique() / len(df_optimized)
                if unique_ratio < 0.5:  # Less than 50% unique values
                    df_optimized[column] = df_optimized[column].astype('category')
            
            elif df_optimized[column].dtype == 'float64':
                # Use float32 for better compression if precision allows
                if df_optimized[column].notna().all():
                    min_val = df_optimized[column].min()
                    max_val = df_optimized[column].max()
                    if min_val >= -3.4e38 and max_val <= 3.4e38:
                        df_optimized[column] = df_optimized[column].astype('float32')
            
            elif df_optimized[column].dtype == 'int64':
                # Use smaller integer types if possible
                min_val = df_optimized[column].min()
                max_val = df_optimized[column].max()
                
                if min_val >= -32768 and max_val <= 32767:
                    df_optimized[column] = df_optimized[column].astype('int16')
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    df_optimized[column] = df_optimized[column].astype('int32')
        
        return df_optimized
    
    def _load_single_file(self, df: pd.DataFrame, output_path: str, 
                         compression: str) -> Dict[str, Any]:
        """
        Load data to a single Parquet file.
        
        Args:
            df: DataFrame to load
            output_path: Output file path
            compression: Compression type
            
        Returns:
            Loading statistics
        """
        stats = {
            'records_loaded': len(df),
            'file_size_mb': 0.0,
            'partition_count': 1,
            'compression_ratio': 0.0
        }
        
        # Save to Parquet
        df.to_parquet(
            output_path,
            compression=compression,
            index=False,
            engine='pyarrow'
        )
        
        # Calculate file size
        stats['file_size_mb'] = os.path.getsize(output_path) / (1024 * 1024)
        
        # Estimate compression ratio (rough calculation)
        estimated_uncompressed = len(df) * len(df.columns) * 8  # Rough estimate
        stats['compression_ratio'] = (1 - stats['file_size_mb'] * 1024 * 1024 / estimated_uncompressed) * 100
        
        return stats
    
    def _load_partitioned_data(self, df: pd.DataFrame, base_output_path: str,
                              partition_by: List[str], compression: str) -> Dict[str, Any]:
        """
        Load data with partitioning.
        
        Args:
            df: DataFrame to load
            base_output_path: Base output path
            partition_by: Columns to partition by
            compression: Compression type
            
        Returns:
            Loading statistics
        """
        stats = {
            'records_loaded': len(df),
            'file_size_mb': 0.0,
            'partition_count': 0,
            'compression_ratio': 0.0
        }
        
        # Remove file extension for partitioning
        base_path = base_output_path.replace('.parquet', '')
        
        # Group by partition columns
        partitions = df.groupby(partition_by)
        total_size = 0.0
        
        for partition_values, partition_df in partitions:
            # Create partition path
            if isinstance(partition_values, tuple):
                partition_dict = dict(zip(partition_by, partition_values))
            else:
                partition_dict = {partition_by[0]: partition_values}
            
            partition_path = self._create_partition_path(base_path, partition_dict)
            
            # Save partition
            partition_df.to_parquet(
                partition_path,
                compression=compression,
                index=False,
                engine='pyarrow'
            )
            
            # Update statistics
            partition_size = os.path.getsize(partition_path) / (1024 * 1024)
            total_size += partition_size
            stats['partition_count'] += 1
        
        stats['file_size_mb'] = total_size
        
        # Estimate compression ratio
        estimated_uncompressed = len(df) * len(df.columns) * 8
        stats['compression_ratio'] = (1 - stats['file_size_mb'] * 1024 * 1024 / estimated_uncompressed) * 100
        
        return stats
    
    def _create_partition_path(self, base_path: str, partition_dict: Dict[str, Any]) -> str:
        """
        Create partition path from partition dictionary.
        
        Args:
            base_path: Base path
            partition_dict: Partition column-value pairs
            
        Returns:
            Partition path
        """
        partition_path = base_path
        
        for column, value in partition_dict.items():
            # Sanitize value for file path
            safe_value = str(value).replace('/', '_').replace('\\', '_')
            partition_path = os.path.join(partition_path, f"{column}={safe_value}")
        
        return f"{partition_path}.parquet"
    
    def load_multiple_files(self, dataframes: List[pd.DataFrame], 
                          base_output_path: Optional[str] = None,
                          partition_by: Optional[List[str]] = None,
                          compression: Optional[str] = None) -> Dict[str, Any]:
        """
        Load multiple DataFrames to storage.
        
        Args:
            dataframes: List of DataFrames to load
            base_output_path: Base output path
            partition_by: Columns to partition by
            compression: Compression type
            
        Returns:
            Loading statistics
        """
        combined_stats = {
            'total_files': len(dataframes),
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'total_size_mb': 0.0,
            'average_compression_ratio': 0.0,
            'file_paths': [],
            'errors': []
        }
        
        if base_output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_output_path = os.path.join(
                self.config.PROCESSED_DATA_PATH,
                f'processed_data_{timestamp}'
            )
        
        compression_ratios = []
        
        for i, df in enumerate(dataframes):
            try:
                # Create individual file path
                file_path = f"{base_output_path}_part_{i:03d}.parquet"
                
                # Load individual file
                stats = self.load_data(df, file_path, partition_by, compression)
                
                if stats['success']:
                    combined_stats['successful_files'] += 1
                    combined_stats['total_records'] += stats['records_loaded']
                    combined_stats['total_size_mb'] += stats['file_size_mb']
                    combined_stats['file_paths'].append(stats['output_path'])
                    
                    if stats['compression_ratio'] > 0:
                        compression_ratios.append(stats['compression_ratio'])
                else:
                    combined_stats['failed_files'] += 1
                    combined_stats['errors'].append(stats['error'])
                    
            except Exception as e:
                combined_stats['failed_files'] += 1
                combined_stats['errors'].append(f"File {i}: {str(e)}")
        
        # Calculate average compression ratio
        if compression_ratios:
            combined_stats['average_compression_ratio'] = sum(compression_ratios) / len(compression_ratios)
        
        self.logger.info(f"Multiple file loading complete: {combined_stats['successful_files']}/{combined_stats['total_files']} files loaded")
        
        return combined_stats
    
    def get_storage_summary(self, data_path: str) -> Dict[str, Any]:
        """
        Get summary of stored data.
        
        Args:
            data_path: Path to stored data
            
        Returns:
            Storage summary
        """
        summary = {
            'total_files': 0,
            'total_size_mb': 0.0,
            'total_records': 0,
            'partition_info': {},
            'file_types': {},
            'last_modified': None
        }
        
        try:
            if os.path.isfile(data_path):
                # Single file
                summary['total_files'] = 1
                summary['total_size_mb'] = os.path.getsize(data_path) / (1024 * 1024)
                summary['last_modified'] = datetime.fromtimestamp(os.path.getmtime(data_path))
                
                # Try to get record count
                try:
                    df = pd.read_parquet(data_path)
                    summary['total_records'] = len(df)
                except:
                    pass
                    
            elif os.path.isdir(data_path):
                # Directory with potentially partitioned data
                for root, dirs, files in os.walk(data_path):
                    for file in files:
                        if file.endswith('.parquet'):
                            file_path = os.path.join(root, file)
                            file_size = os.path.getsize(file_path) / (1024 * 1024)
                            
                            summary['total_files'] += 1
                            summary['total_size_mb'] += file_size
                            
                            # Update last modified
                            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            if summary['last_modified'] is None or file_mtime > summary['last_modified']:
                                summary['last_modified'] = file_mtime
                            
                            # Try to get record count
                            try:
                                df = pd.read_parquet(file_path)
                                summary['total_records'] += len(df)
                            except:
                                pass
                            
                            # Analyze partition structure
                            rel_path = os.path.relpath(file_path, data_path)
                            if '=' in rel_path:
                                partition_parts = rel_path.split(os.sep)
                                for part in partition_parts[:-1]:  # Exclude filename
                                    if '=' in part:
                                        col, val = part.split('=', 1)
                                        if col not in summary['partition_info']:
                                            summary['partition_info'][col] = set()
                                        summary['partition_info'][col].add(val)
                
                # Convert sets to lists for JSON serialization
                for col in summary['partition_info']:
                    summary['partition_info'][col] = list(summary['partition_info'][col])
        
        except Exception as e:
            self.logger.error(f"Error getting storage summary: {e}")
            summary['error'] = str(e)
        
        return summary


def main():
    """Main function for testing the loading module."""
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
    
    # Transform data first
    transformer = DataTransformation()
    transformed_df, _ = transformer.transform_data(sample_data)
    
    # Initialize loading
    loader = DataLoading()
    
    # Load data
    loading_stats = loader.load_data(
        transformed_df,
        partition_by=['date', 'reading_type'],
        compression='snappy'
    )
    
    print("Loading Results:")
    print(f"Success: {loading_stats['success']}")
    print(f"Records loaded: {loading_stats['records_loaded']}")
    print(f"File size: {loading_stats['file_size_mb']:.2f} MB")
    print(f"Partition count: {loading_stats['partition_count']}")
    print(f"Compression ratio: {loading_stats['compression_ratio']:.2f}%")
    print(f"Loading time: {loading_stats['loading_time_seconds']:.2f} seconds")
    print(f"Output path: {loading_stats['output_path']}")
    
    if loading_stats['success']:
        # Get storage summary
        summary = loader.get_storage_summary(loading_stats['output_path'])
        print(f"\nStorage Summary:")
        print(f"Total files: {summary['total_files']}")
        print(f"Total size: {summary['total_size_mb']:.2f} MB")
        print(f"Total records: {summary['total_records']}")
        if summary['partition_info']:
            print(f"Partitions: {summary['partition_info']}")


if __name__ == "__main__":
    main()
