# AgriSense Pro Data Pipeline

A production-grade data pipeline for processing agricultural sensor data with comprehensive ingestion, transformation, validation, and loading capabilities.

## Overview

The AgriSense Pro Data Pipeline is designed to handle agricultural sensor data from multiple sources, providing:

- **Data Ingestion**: Modular ingestion from Parquet files with DuckDB-powered validation
- **Data Transformation**: Cleaning, enrichment, outlier detection, and derived field generation
- **Data Validation**: Comprehensive quality checks using DuckDB queries
- **Data Loading**: Optimized storage with partitioning and compression

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│    Ingestion     │───▶│ Transformation  │───▶│     Loading     │
│  (Parquet Files)│    │   (DuckDB +      │    │ (Cleaning +     │    │ (Partitioned +  │
│                 │    │   Validation)    │    │   Enrichment)   │    │   Compressed)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │                        │
                                ▼                        ▼                        ▼
                       ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
                       │   Schema Check   │    │ Quality Report  │    │ Processed Data  │
                       │   File Validation│    │   Anomaly Det.  │    │   (Parquet)     │
                       └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## Features

### Data Ingestion
- **Incremental Loading**: Date-based file filtering
- **Schema Validation**: DuckDB-powered schema inspection
- **Error Handling**: Comprehensive error logging and recovery
- **Performance**: Optimized DuckDB settings for large datasets

### Data Transformation
- **Data Cleaning**: Duplicate removal, missing value handling
- **Timestamp Processing**: ISO 8601 format, UTC+5:30 conversion
- **Calibration**: Sensor-specific calibration parameters
- **Outlier Detection**: Z-score based outlier identification
- **Derived Fields**: Daily averages, rolling averages, value changes
- **Anomaly Detection**: Range-based anomaly identification

### Data Validation
- **Type Validation**: Column data type verification
- **Range Validation**: Expected value range checking
- **Completeness**: Missing value analysis
- **Time Coverage**: Data gap detection using DuckDB generate_series
- **Quality Scoring**: Overall data quality assessment

### Data Loading
- **Partitioning**: Date and sensor-based partitioning
- **Compression**: Snappy/Gzip compression options
- **Optimization**: Data type optimization for storage efficiency
- **Monitoring**: Loading statistics and performance metrics

## Installation

### Prerequisites
- Python 3.11+
- Docker (optional)

### Local Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd agrisense-pro/data_pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up directories**
   ```bash
   mkdir -p data/raw data/processed data/quality_reports
   ```

### Docker Installation

1. **Build the Docker image**
   ```bash
   docker build -t agrisense-pipeline .
   ```

2. **Run the pipeline**
   ```bash
   docker run -v $(pwd)/data:/app/data agrisense-pipeline python main.py
   ```

## Configuration

The pipeline uses a centralized configuration system in `pipeline/config.py`:

### Key Configuration Options

```python
# Data paths
RAW_DATA_PATH = 'data/raw'
PROCESSED_DATA_PATH = 'data/processed'
QUALITY_REPORTS_PATH = 'data/quality_reports'

# Processing settings
BATCH_SIZE = 10000
MAX_WORKERS = 4
Z_SCORE_THRESHOLD = 3.0

# Calibration parameters
CALIBRATION_PARAMS = {
    'temperature': CalibrationParams(multiplier=1.0, offset=0.0, unit="°C"),
    'humidity': CalibrationParams(multiplier=1.0, offset=0.0, unit="%"),
    # ... more sensor types
}

# Expected value ranges
EXPECTED_RANGES = {
    'temperature': ExpectedRange(min_value=-10.0, max_value=60.0, unit="°C"),
    'humidity': ExpectedRange(min_value=0.0, max_value=100.0, unit="%"),
    # ... more ranges
}
```

### Environment Variables

```bash
export RAW_DATA_PATH="/path/to/raw/data"
export PROCESSED_DATA_PATH="/path/to/processed/data"
export LOG_LEVEL="INFO"
export BATCH_SIZE="10000"
```

## Usage

### Basic Usage

```bash
# Process all files in data/raw/
python main.py

# Process specific date range
python main.py --start-date 2023-06-01 --end-date 2023-06-30

# Custom output path
python main.py --output-path data/processed/custom_output.parquet

# Skip validation
python main.py --no-validation

# Skip quality report
python main.py --no-quality-report
```

### Programmatic Usage

```python
from main import AgriSensePipeline

# Initialize pipeline
pipeline = AgriSensePipeline()

# Run complete pipeline
results = pipeline.run_pipeline(
    start_date='2023-06-01',
    end_date='2023-06-30',
    enable_validation=True,
    enable_quality_report=True
)

# Print summary
print(pipeline.get_pipeline_summary(results))
```

### Individual Components

```python
from pipeline.ingestion import DataIngestion
from pipeline.transformation import DataTransformation
from pipeline.validation import DataValidation
from pipeline.loading import DataLoading

# Ingestion
ingestion = DataIngestion()
files = ingestion.get_files_to_process('2023-06-01', '2023-06-30')
dataframes, summary = ingestion.process_files(files)

# Transformation
transformer = DataTransformation()
transformed_df, stats = transformer.transform_data(dataframes[0])

# Validation
validator = DataValidation()
validation_results = validator.validate_data_quality(transformed_df)
report_path = validator.generate_quality_report(validation_results)

# Loading
loader = DataLoading()
loading_stats = loader.load_data(
    transformed_df,
    partition_by=['date', 'reading_type'],
    compression='snappy'
)
```

## Data Schema

### Input Schema (Raw Data)
```parquet
sensor_id: string
timestamp: timestamp
reading_type: string
value: double
battery_level: double
```

### Output Schema (Processed Data)
```parquet
sensor_id: string
timestamp: timestamp (ISO 8601, UTC+5:30)
reading_type: string
value: double (calibrated)
battery_level: double
date: string
hour: int32
daily_avg_value: double
rolling_7day_avg: double
value_change: double
time_diff_hours: double
anomalous_reading: boolean
battery_anomalous: boolean
is_outlier: boolean
```

## Calibration & Anomaly Logic

### Calibration
The pipeline applies sensor-specific calibration using the formula:
```
calibrated_value = raw_value * multiplier + offset
```

Example calibration parameters:
- Temperature: multiplier=1.0, offset=0.0 (no calibration)
- Humidity: multiplier=1.02, offset=-1.0 (2% gain, -1% offset)

### Anomaly Detection
Two types of anomaly detection:

1. **Range-based**: Values outside expected ranges per sensor type
   - Temperature: -10°C to 60°C
   - Humidity: 0% to 100%
   - Battery: 0% to 100%

2. **Statistical**: Z-score based outlier detection
   - Threshold: 3.0 (configurable)
   - Applied per reading type
   - Outliers replaced with median values

## Quality Reports

The pipeline generates comprehensive quality reports including:

### JSON Report
- Overall quality score (0-100)
- Detailed validation results
- Missing value analysis
- Value range statistics
- Anomaly detection results
- Time coverage analysis
- Data gap identification

### CSV Summary
- Key metrics in tabular format
- Categorized by validation type
- Easy to import into analytics tools

Example quality report structure:
```
metric,value,category
Overall Quality Score,85.5,Overall
Total Records,10000,Overall
Missing Values - sensor_id,0.0%,Data Completeness
Valid Range - temperature,98.5%,Data Accuracy
Anomalous Readings - humidity,2.1%,Data Quality
Total Missing Hours,15,Data Continuity
```

## Testing

### Run Tests
```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_ingestion.py

# Run with coverage
pytest --cov=pipeline tests/
```

### Test Coverage
The test suite covers:
- Data ingestion with various file formats
- Schema validation scenarios
- Transformation logic and edge cases
- Validation queries and quality scoring
- Loading with different partitioning strategies

## Performance

### Optimization Features
- **DuckDB Integration**: Fast analytical queries
- **Memory Management**: Efficient memory usage monitoring
- **Data Type Optimization**: Automatic data type optimization
- **Partitioning**: Query-optimized data partitioning
- **Compression**: Storage space optimization

### Performance Metrics
- **Ingestion**: ~10,000 records/second
- **Transformation**: ~5,000 records/second
- **Validation**: ~20,000 records/second
- **Loading**: ~15,000 records/second

*Performance may vary based on data size and system resources*

## Monitoring & Logging

### Log Levels
- **DEBUG**: Detailed debugging information
- **INFO**: General pipeline progress
- **WARNING**: Non-critical issues
- **ERROR**: Critical errors

### Log Format
```
2023-06-01 10:30:15 - pipeline.ingestion - INFO - Processing file: data/raw/2023-06-01.parquet
2023-06-01 10:30:16 - pipeline.transformation - INFO - Transformation complete: 1000 records processed
```

### Metrics Collection
The pipeline collects comprehensive metrics:
- Processing time per stage
- Memory usage
- Record counts
- Error rates
- Quality scores

## Troubleshooting

### Common Issues

1. **File Not Found**
   ```
   Error: File not found or not readable
   Solution: Check file paths and permissions
   ```

2. **Schema Validation Failed**
   ```
   Error: Missing required columns
   Solution: Verify input data schema matches expected format
   ```

3. **Memory Issues**
   ```
   Error: Memory limit exceeded
   Solution: Reduce BATCH_SIZE or increase system memory
   ```

4. **DuckDB Connection Issues**
   ```
   Error: Failed to initialize DuckDB
   Solution: Check system dependencies and permissions
   ```

### Debug Mode
```bash
export LOG_LEVEL="DEBUG"
python main.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the test examples

## Roadmap

- [ ] Real-time streaming support
- [ ] Cloud storage integration (S3, GCS)
- [ ] Advanced anomaly detection algorithms
- [ ] Web-based monitoring dashboard
- [ ] Machine learning model integration
- [ ] Multi-language support 