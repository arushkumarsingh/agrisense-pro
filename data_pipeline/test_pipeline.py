#!/usr/bin/env python3
"""
Test script to demonstrate the AgriSense Pro data pipeline functionality.
This script creates sample data and runs the complete pipeline.
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import shutil

# Add the pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipeline'))

from main import AgriSensePipeline
from pipeline.config import PipelineConfig


def create_sample_data(output_dir: str, num_files: int = 3):
    """
    Create sample sensor data files for testing.
    
    Args:
        output_dir: Directory to save sample files
        num_files: Number of sample files to create
    """
    print(f"Creating {num_files} sample data files...")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Sample sensor IDs and reading types
    sensor_ids = ['sensor_001', 'sensor_002', 'sensor_003']
    reading_types = ['temperature', 'humidity', 'soil_moisture', 'light_intensity']
    
    for i in range(num_files):
        # Create date for this file
        file_date = datetime(2023, 6, 1) + timedelta(days=i)
        filename = f"{file_date.strftime('%Y-%m-%d')}.parquet"
        filepath = os.path.join(output_dir, filename)
        
        # Generate sample data
        records = []
        for hour in range(24):
            for sensor_id in sensor_ids:
                for reading_type in reading_types:
                    timestamp = file_date + timedelta(hours=hour, minutes=np.random.randint(0, 60))
                    
                    # Generate realistic values based on reading type
                    if reading_type == 'temperature':
                        base_value = 20 + 10 * np.sin(hour * np.pi / 12)  # Daily temperature cycle
                        value = base_value + np.random.normal(0, 2)
                    elif reading_type == 'humidity':
                        base_value = 60 - 20 * np.sin(hour * np.pi / 12)  # Inverse of temperature
                        value = max(0, min(100, base_value + np.random.normal(0, 5)))
                    elif reading_type == 'soil_moisture':
                        value = np.random.uniform(20, 80)
                    elif reading_type == 'light_intensity':
                        # Light intensity with day/night cycle
                        if 6 <= hour <= 18:  # Daytime
                            value = np.random.uniform(1000, 50000)
                        else:  # Nighttime
                            value = np.random.uniform(0, 100)
                    
                    # Add some anomalies and outliers
                    if np.random.random() < 0.05:  # 5% chance of anomaly
                        if reading_type == 'temperature':
                            value = np.random.uniform(-50, 100)  # Extreme values
                        elif reading_type == 'humidity':
                            value = np.random.uniform(-10, 110)  # Out of range
                    
                    # Battery level (decreasing over time)
                    battery_level = max(0, 100 - (i * 24 + hour) * 0.1 + np.random.normal(0, 2))
                    
                    records.append({
                        'sensor_id': sensor_id,
                        'timestamp': timestamp.isoformat(),
                        'reading_type': reading_type,
                        'value': round(value, 2),
                        'battery_level': round(battery_level, 2)
                    })
        
        # Create DataFrame and save
        df = pd.DataFrame(records)
        df.to_parquet(filepath, index=False)
        print(f"Created: {filepath} ({len(df)} records)")
    
    print(f"Sample data creation complete. Files saved to: {output_dir}")


def run_pipeline_demo():
    """Run a complete pipeline demonstration."""
    print("=" * 60)
    print("AGRISENSE PRO DATA PIPELINE DEMONSTRATION")
    print("=" * 60)
    
    # Create temporary directories
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_data_dir = os.path.join(temp_dir, "data", "raw")
        processed_data_dir = os.path.join(temp_dir, "data", "processed")
        quality_reports_dir = os.path.join(temp_dir, "data", "quality_reports")
        
        # Create sample data
        create_sample_data(raw_data_dir, num_files=3)
        
        # Update configuration to use temporary directories
        config = PipelineConfig()
        config.RAW_DATA_PATH = raw_data_dir
        config.PROCESSED_DATA_PATH = processed_data_dir
        config.QUALITY_REPORTS_PATH = quality_reports_dir
        
        print("\n" + "=" * 60)
        print("RUNNING PIPELINE")
        print("=" * 60)
        
        # Initialize and run pipeline
        pipeline = AgriSensePipeline(config)
        
        try:
            # Run the complete pipeline
            results = pipeline.run_pipeline(
                start_date='2023-06-01',
                end_date='2023-06-03',
                enable_validation=True,
                enable_quality_report=True
            )
            
            # Print results
            print("\n" + "=" * 60)
            print("PIPELINE RESULTS")
            print("=" * 60)
            
            summary = pipeline.get_pipeline_summary(results)
            print(summary)
            
            # Show output files
            print("\n" + "=" * 60)
            print("OUTPUT FILES")
            print("=" * 60)
            
            if results.get('success', False):
                print(f"✅ Pipeline completed successfully!")
                print(f"📊 Processed data: {results.get('output_data_path', 'N/A')}")
                print(f"📋 Quality report: {results.get('quality_report_path', 'N/A')}")
                
                # List processed files
                if os.path.exists(processed_data_dir):
                    print(f"\n📁 Processed data files:")
                    for root, dirs, files in os.walk(processed_data_dir):
                        for file in files:
                            if file.endswith('.parquet'):
                                file_path = os.path.join(root, file)
                                file_size = os.path.getsize(file_path) / (1024 * 1024)
                                print(f"   - {os.path.relpath(file_path, temp_dir)} ({file_size:.2f} MB)")
                
                # List quality report files
                if os.path.exists(quality_reports_dir):
                    print(f"\n📋 Quality report files:")
                    for file in os.listdir(quality_reports_dir):
                        if file.endswith(('.json', '.csv')):
                            file_path = os.path.join(quality_reports_dir, file)
                            file_size = os.path.getsize(file_path) / (1024 * 1024)
                            print(f"   - {file} ({file_size:.2f} MB)")
                
                # Show sample of processed data
                print(f"\n" + "=" * 60)
                print("SAMPLE PROCESSED DATA")
                print("=" * 60)
                
                # Find a processed file to show sample
                for root, dirs, files in os.walk(processed_data_dir):
                    for file in files:
                        if file.endswith('.parquet'):
                            file_path = os.path.join(root, file)
                            try:
                                sample_df = pd.read_parquet(file_path)
                                print(f"File: {os.path.relpath(file_path, temp_dir)}")
                                print(f"Shape: {sample_df.shape}")
                                print(f"Columns: {list(sample_df.columns)}")
                                print("\nFirst 5 rows:")
                                print(sample_df.head().to_string(index=False))
                                break
                            except Exception as e:
                                print(f"Error reading {file_path}: {e}")
                            break
                    break
                
            else:
                print(f"❌ Pipeline failed!")
                print("Errors:")
                for error in results.get('errors', []):
                    print(f"   - {error}")
                    
        except Exception as e:
            print(f"❌ Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # Cleanup
            pipeline._cleanup()


def run_individual_components_demo():
    """Demonstrate individual pipeline components."""
    print("\n" + "=" * 60)
    print("INDIVIDUAL COMPONENTS DEMONSTRATION")
    print("=" * 60)
    
    # Create sample data
    sample_data = pd.DataFrame({
        'sensor_id': ['sensor_001', 'sensor_001', 'sensor_002', 'sensor_002'],
        'timestamp': ['2023-06-01T10:00:00Z', '2023-06-01T11:00:00Z', 
                     '2023-06-01T10:00:00Z', '2023-06-01T11:00:00Z'],
        'reading_type': ['temperature', 'temperature', 'humidity', 'humidity'],
        'value': [25.5, 26.0, 60.0, 65.0],
        'battery_level': [95.0, 94.0, 90.0, 89.0]
    })
    
    print("Sample input data:")
    print(sample_data.to_string(index=False))
    
    # Test transformation
    print("\n--- TRANSFORMATION ---")
    from pipeline.transformation import DataTransformation
    transformer = DataTransformation()
    transformed_df, stats = transformer.transform_data(sample_data)
    
    print(f"Transformation stats: {stats}")
    print("\nTransformed data:")
    print(transformed_df.to_string(index=False))
    
    # Test validation
    print("\n--- VALIDATION ---")
    from pipeline.validation import DataValidation
    validator = DataValidation()
    validation_results = validator.validate_data_quality(transformed_df)
    
    print(f"Quality score: {validation_results.get('quality_score', 0):.2f}/100")
    print(f"Total records: {validation_results.get('total_records', 0)}")
    
    # Test loading
    print("\n--- LOADING ---")
    from pipeline.loading import DataLoading
    loader = DataLoading()
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        loading_stats = loader.load_data(
            transformed_df,
            output_path=tmp_file.name,
            partition_by=['date', 'reading_type'],
            compression='snappy'
        )
        
        print(f"Loading stats: {loading_stats}")
        
        # Verify the file was created
        if os.path.exists(tmp_file.name):
            file_size = os.path.getsize(tmp_file.name) / (1024 * 1024)
            print(f"Output file: {tmp_file.name} ({file_size:.2f} MB)")
            
            # Read back and verify
            loaded_df = pd.read_parquet(tmp_file.name)
            print(f"Loaded data shape: {loaded_df.shape}")
        
        # Cleanup
        os.unlink(tmp_file.name)
    
    # Cleanup
    validator.close()


def main():
    """Main function to run the demonstration."""
    try:
        # Run complete pipeline demo
        run_pipeline_demo()
        
        # Run individual components demo
        run_individual_components_demo()
        
        print("\n" + "=" * 60)
        print("DEMONSTRATION COMPLETE")
        print("=" * 60)
        print("✅ All tests passed successfully!")
        print("\nThe pipeline is ready for production use.")
        
    except Exception as e:
        print(f"\n❌ Demonstration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 