"""
Main pipeline orchestrator for AgriSense Pro data pipeline.
Coordinates ingestion, transformation, validation, and loading components.
"""
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd
from pathlib import Path

# Add the pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipeline'))

from pipeline.config import PipelineConfig
from pipeline.ingestion import DataIngestion
from pipeline.transformation import DataTransformation
from pipeline.validation import DataValidation
from pipeline.loading import DataLoading
from pipeline.utils import setup_logging, ensure_directory_exists, save_json_report


class AgriSensePipeline:
    """Main pipeline orchestrator for AgriSense Pro data processing."""
    
    def __init__(self, config: PipelineConfig = None):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config or PipelineConfig()
        self.logger = setup_logging(__name__, self.config.LOG_LEVEL)
        
        # Initialize pipeline components
        self.ingestion = DataIngestion(self.config)
        self.transformation = DataTransformation(self.config)
        self.validation = DataValidation(self.config)
        self.loading = DataLoading(self.config)
        
        self.logger.info("AgriSense Pipeline initialized successfully")
    
    def run_pipeline(self, start_date: Optional[str] = None, 
                    end_date: Optional[str] = None,
                    output_path: Optional[str] = None,
                    enable_validation: bool = True,
                    enable_quality_report: bool = True) -> Dict[str, Any]:
        """
        Run the complete data pipeline.
        
        Args:
            start_date: Start date for processing (YYYY-MM-DD)
            end_date: End date for processing (YYYY-MM-DD)
            output_path: Custom output path for processed data
            enable_validation: Whether to run data validation
            enable_quality_report: Whether to generate quality report
            
        Returns:
            Pipeline execution results
        """
        pipeline_results = {
            'pipeline_start_time': datetime.now().isoformat(),
            'pipeline_end_time': None,
            'success': False,
            'total_records_processed': 0,
            'ingestion_results': {},
            'transformation_results': {},
            'validation_results': {},
            'loading_results': {},
            'quality_report_path': None,
            'output_data_path': None,
            'errors': []
        }
        
        try:
            self.logger.info("Starting AgriSense data pipeline")
            
            # Step 1: Data Ingestion
            self.logger.info("Step 1: Data Ingestion")
            ingestion_results = self._run_ingestion(start_date, end_date)
            pipeline_results['ingestion_results'] = ingestion_results
            
            if not ingestion_results.get('success', False):
                raise Exception(f"Ingestion failed: {ingestion_results.get('errors', [])}")
            
            # Step 2: Data Transformation
            self.logger.info("Step 2: Data Transformation")
            transformation_results = self._run_transformation(ingestion_results['dataframes'])
            pipeline_results['transformation_results'] = transformation_results
            
            if not transformation_results.get('success', False):
                raise Exception(f"Transformation failed: {transformation_results.get('error', 'Unknown error')}")
            
            # Step 3: Data Validation (optional)
            if enable_validation:
                self.logger.info("Step 3: Data Validation")
                validation_results = self._run_validation(transformation_results['transformed_data'])
                pipeline_results['validation_results'] = validation_results
                
                # Generate quality report if requested
                if enable_quality_report and validation_results.get('success', False):
                    quality_report_path = self._generate_quality_report(validation_results)
                    pipeline_results['quality_report_path'] = quality_report_path
            
            # Step 4: Data Loading
            self.logger.info("Step 4: Data Loading")
            loading_results = self._run_loading(
                transformation_results['transformed_data'], 
                output_path
            )
            pipeline_results['loading_results'] = loading_results
            
            if not loading_results.get('success', False):
                raise Exception(f"Loading failed: {loading_results.get('error', 'Unknown error')}")
            
            # Update final results
            pipeline_results['pipeline_end_time'] = datetime.now().isoformat()
            pipeline_results['success'] = True
            pipeline_results['total_records_processed'] = loading_results.get('records_loaded', 0)
            pipeline_results['output_data_path'] = loading_results.get('output_path', '')
            
            self.logger.info(f"Pipeline completed successfully. Processed {pipeline_results['total_records_processed']} records")
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            self.logger.error(error_msg)
            pipeline_results['errors'].append(error_msg)
            pipeline_results['pipeline_end_time'] = datetime.now().isoformat()
            pipeline_results['success'] = False
            
        finally:
            # Clean up resources
            self._cleanup()
        
        return pipeline_results
    
    def _run_ingestion(self, start_date: Optional[str], end_date: Optional[str]) -> Dict[str, Any]:
        """Run the ingestion step."""
        try:
            # Get files to process
            files = self.ingestion.get_files_to_process(start_date, end_date)
            
            if not files:
                return {
                    'success': False,
                    'errors': ['No files found to process'],
                    'dataframes': []
                }
            
            # Process files
            dataframes, summary = self.ingestion.process_files(files)
            
            return {
                'success': summary['successful_files'] > 0,
                'files_processed': summary['total_files'],
                'files_successful': summary['successful_files'],
                'files_failed': summary['failed_files'],
                'total_records': summary['total_records'],
                'valid_records': summary['valid_records'],
                'invalid_records': summary['invalid_records'],
                'total_size_mb': summary['total_size_mb'],
                'errors': summary['errors'],
                'dataframes': dataframes
            }
            
        except Exception as e:
            return {
                'success': False,
                'errors': [str(e)],
                'dataframes': []
            }
    
    def _run_transformation(self, dataframes: List[pd.DataFrame]) -> Dict[str, Any]:
        """Run the transformation step."""
        try:
            if not dataframes:
                return {
                    'success': False,
                    'error': 'No data to transform',
                    'transformed_data': None
                }
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            # Transform data
            transformed_df, transformation_stats = self.transformation.transform_data(combined_df)
            
            return {
                'success': 'error' not in transformation_stats,
                'original_records': transformation_stats.get('original_records', 0),
                'final_records': transformation_stats.get('records_final', 0),
                'duplicates_removed': transformation_stats.get('duplicates_removed', 0),
                'missing_values_filled': transformation_stats.get('missing_values_filled', 0),
                'outliers_detected': transformation_stats.get('outliers_detected', 0),
                'anomalous_readings': transformation_stats.get('anomalous_readings', 0),
                'derived_fields': transformation_stats.get('derived_fields_added', []),
                'error': transformation_stats.get('error'),
                'transformed_data': transformed_df
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'transformed_data': None
            }
    
    def _run_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run the validation step."""
        try:
            if df is None or len(df) == 0:
                return {
                    'success': False,
                    'error': 'No data to validate'
                }
            
            # Validate data quality
            validation_results = self.validation.validate_data_quality(df)
            
            return {
                'success': validation_results.get('quality_score', 0) > 0,
                'quality_score': validation_results.get('quality_score', 0),
                'total_records': validation_results.get('total_records', 0),
                'missing_values': validation_results.get('missing_values', {}),
                'value_ranges': validation_results.get('value_ranges', {}),
                'anomalies': validation_results.get('anomalies', {}),
                'data_gaps': validation_results.get('data_gaps', {}),
                'validation_results': validation_results
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _run_loading(self, df: pd.DataFrame, output_path: Optional[str]) -> Dict[str, Any]:
        """Run the loading step."""
        try:
            if df is None or len(df) == 0:
                return {
                    'success': False,
                    'error': 'No data to load'
                }
            
            # Load data
            loading_stats = self.loading.load_data(
                df,
                output_path=output_path,
                partition_by=self.config.DEFAULT_JOB_CONFIG.get('partition_by', ['date']),
                compression=self.config.DEFAULT_JOB_CONFIG.get('compression', 'snappy')
            )
            
            return loading_stats
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _generate_quality_report(self, validation_results: Dict[str, Any]) -> str:
        """Generate quality report."""
        try:
            if 'validation_results' in validation_results:
                report_path = self.validation.generate_quality_report(
                    validation_results['validation_results']
                )
                self.logger.info(f"Quality report generated: {report_path}")
                return report_path
            else:
                self.logger.warning("No validation results available for quality report")
                return ""
        except Exception as e:
            self.logger.error(f"Error generating quality report: {e}")
            return ""
    
    def _cleanup(self):
        """Clean up resources."""
        try:
            if hasattr(self, 'ingestion'):
                self.ingestion.close()
            if hasattr(self, 'validation'):
                self.validation.close()
            self.logger.info("Pipeline cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def get_pipeline_summary(self, results: Dict[str, Any]) -> str:
        """Generate a human-readable pipeline summary."""
        summary_lines = []
        
        summary_lines.append("=" * 60)
        summary_lines.append("AGRISENSE PRO DATA PIPELINE SUMMARY")
        summary_lines.append("=" * 60)
        
        # Overall status
        status = "SUCCESS" if results.get('success', False) else "FAILED"
        summary_lines.append(f"Status: {status}")
        summary_lines.append(f"Start Time: {results.get('pipeline_start_time', 'N/A')}")
        summary_lines.append(f"End Time: {results.get('pipeline_end_time', 'N/A')}")
        summary_lines.append(f"Total Records Processed: {results.get('total_records_processed', 0):,}")
        
        # Ingestion results
        ingestion = results.get('ingestion_results', {})
        if ingestion:
            summary_lines.append("\n--- INGESTION RESULTS ---")
            summary_lines.append(f"Files Processed: {ingestion.get('files_processed', 0)}")
            summary_lines.append(f"Files Successful: {ingestion.get('files_successful', 0)}")
            summary_lines.append(f"Files Failed: {ingestion.get('files_failed', 0)}")
            summary_lines.append(f"Valid Records: {ingestion.get('valid_records', 0):,}")
            summary_lines.append(f"Invalid Records: {ingestion.get('invalid_records', 0):,}")
            summary_lines.append(f"Total Size: {ingestion.get('total_size_mb', 0):.2f} MB")
        
        # Transformation results
        transformation = results.get('transformation_results', {})
        if transformation:
            summary_lines.append("\n--- TRANSFORMATION RESULTS ---")
            summary_lines.append(f"Original Records: {transformation.get('original_records', 0):,}")
            summary_lines.append(f"Final Records: {transformation.get('final_records', 0):,}")
            summary_lines.append(f"Duplicates Removed: {transformation.get('duplicates_removed', 0):,}")
            summary_lines.append(f"Missing Values Filled: {transformation.get('missing_values_filled', 0):,}")
            summary_lines.append(f"Outliers Detected: {transformation.get('outliers_detected', 0):,}")
            summary_lines.append(f"Anomalous Readings: {transformation.get('anomalous_readings', 0):,}")
            summary_lines.append(f"Derived Fields: {', '.join(transformation.get('derived_fields', []))}")
        
        # Validation results
        validation = results.get('validation_results', {})
        if validation:
            summary_lines.append("\n--- VALIDATION RESULTS ---")
            summary_lines.append(f"Quality Score: {validation.get('quality_score', 0):.2f}/100")
            summary_lines.append(f"Total Records: {validation.get('total_records', 0):,}")
        
        # Loading results
        loading = results.get('loading_results', {})
        if loading:
            summary_lines.append("\n--- LOADING RESULTS ---")
            summary_lines.append(f"Records Loaded: {loading.get('records_loaded', 0):,}")
            summary_lines.append(f"File Size: {loading.get('file_size_mb', 0):.2f} MB")
            summary_lines.append(f"Partition Count: {loading.get('partition_count', 0)}")
            summary_lines.append(f"Compression Ratio: {loading.get('compression_ratio', 0):.2f}%")
            summary_lines.append(f"Loading Time: {loading.get('loading_time_seconds', 0):.2f} seconds")
            summary_lines.append(f"Output Path: {loading.get('output_path', 'N/A')}")
        
        # Quality report
        if results.get('quality_report_path'):
            summary_lines.append(f"\nQuality Report: {results['quality_report_path']}")
        
        # Errors
        errors = results.get('errors', [])
        if errors:
            summary_lines.append("\n--- ERRORS ---")
            for error in errors:
                summary_lines.append(f"• {error}")
        
        summary_lines.append("=" * 60)
        
        return "\n".join(summary_lines)


def main():
    """Main function to run the pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='AgriSense Pro Data Pipeline')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-path', type=str, help='Custom output path')
    parser.add_argument('--no-validation', action='store_true', help='Skip data validation')
    parser.add_argument('--no-quality-report', action='store_true', help='Skip quality report generation')
    parser.add_argument('--config-file', type=str, help='Path to configuration file')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = AgriSensePipeline()
    
    # Run pipeline
    results = pipeline.run_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        output_path=args.output_path,
        enable_validation=not args.no_validation,
        enable_quality_report=not args.no_quality_report
    )
    
    # Print summary
    summary = pipeline.get_pipeline_summary(results)
    print(summary)
    
    # Exit with appropriate code
    sys.exit(0 if results.get('success', False) else 1)


if __name__ == "__main__":
    main()
