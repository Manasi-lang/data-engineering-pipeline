import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.extract.csv_extractor import CSVExtractor
from src.extract.api_extractor import APIExtractor
from src.transform.data_transformer import DataTransformer
from src.load.database_loader import DatabaseLoader
from config.config import config

class ETLPipeline:
    def __init__(self, pipeline_name: str = "default_pipeline"):
        self.pipeline_name = pipeline_name
        self.logger = self._setup_logger()
        self.pipeline_log = []
        
        # Initialize components
        self.csv_extractor = CSVExtractor()
        self.api_extractor = APIExtractor(config.API_BASE_URL, config.API_KEY)
        self.transformer = DataTransformer()
        self.db_loader = DatabaseLoader(config.DATABASE_URL)
        
    def _setup_logger(self):
        logger = logging.getLogger(f"{__name__}.{self.pipeline_name}")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _log_pipeline_step(self, step: str, status: str, details: Dict = None):
        log_entry = {
            'timestamp': datetime.now(),
            'pipeline_name': self.pipeline_name,
            'step': step,
            'status': status,
            'details': details or {}
        }
        self.pipeline_log.append(log_entry)
        
        status_emoji = "✅" if status == "success" else "❌" if status == "error" else "⏳"
        self.logger.info(f"{status_emoji} {step}: {status} - {details}")
    
    def extract_csv_data(self, file_paths: List[str], table_name: str) -> pd.DataFrame:
        try:
            self._log_pipeline_step("extract_csv", "started", {"files": file_paths, "table": table_name})
            
            # Extract data from multiple CSV files
            df = self.csv_extractor.extract_multiple_csv(file_paths, combine=True)
            
            self._log_pipeline_step("extract_csv", "success", {
                "rows_extracted": len(df),
                "columns": list(df.columns),
                "files_processed": len(file_paths)
            })
            
            return df
            
        except Exception as e:
            self._log_pipeline_step("extract_csv", "error", {"error": str(e)})
            raise
    
    def extract_api_data(self, endpoint: str, table_name: str, params: Dict = None) -> pd.DataFrame:
        try:
            self._log_pipeline_step("extract_api", "started", {"endpoint": endpoint, "table": table_name})
            
            # Extract data from API
            df = self.api_extractor.extract_to_dataframe(endpoint, params)
            
            self._log_pipeline_step("extract_api", "success", {
                "rows_extracted": len(df),
                "columns": list(df.columns),
                "endpoint": endpoint
            })
            
            return df
            
        except Exception as e:
            self._log_pipeline_step("extract_api", "error", {"error": str(e)})
            raise
    
    def transform_data(self, df: pd.DataFrame, table_name: str, 
                      transformation_config: Dict = None) -> pd.DataFrame:
        try:
            self._log_pipeline_step("transform_data", "started", {"table": table_name})
            
            transformed_df = df.copy()
            
            # Apply standard transformations
            transformed_df = self.transformer.clean_data(transformed_df)
            transformed_df = self.transformer.standardize_columns(transformed_df)
            
            # Apply custom transformations if provided
            if transformation_config:
                if 'type_mapping' in transformation_config:
                    transformed_df = self.transformer.convert_data_types(
                        transformed_df, transformation_config['type_mapping']
                    )
                
                if 'filters' in transformation_config:
                    transformed_df = self.transformer.filter_data(
                        transformed_df, transformation_config['filters']
                    )
                
                if 'features' in transformation_config:
                    transformed_df = self.transformer.create_features(
                        transformed_df, transformation_config['features']
                    )
                
                if 'aggregation' in transformation_config:
                    agg_config = transformation_config['aggregation']
                    transformed_df = self.transformer.aggregate_data(
                        transformed_df, 
                        agg_config['group_by'], 
                        agg_config['aggregations']
                    )
            
            self._log_pipeline_step("transform_data", "success", {
                "rows_before": len(df),
                "rows_after": len(transformed_df),
                "columns_before": len(df.columns),
                "columns_after": len(transformed_df.columns)
            })
            
            return transformed_df
            
        except Exception as e:
            self._log_pipeline_step("transform_data", "error", {"error": str(e)})
            raise
    
    def load_data(self, df: pd.DataFrame, table_name: str, 
                  load_strategy: str = 'append') -> bool:
        try:
            self._log_pipeline_step("load_data", "started", {
                "table": table_name,
                "strategy": load_strategy,
                "rows_to_load": len(df)
            })
            
            success = self.db_loader.load_dataframe(
                df, table_name, if_exists=load_strategy, chunk_size=1000
            )
            
            if success:
                self._log_pipeline_step("load_data", "success", {
                    "rows_loaded": len(df),
                    "table": table_name
                })
            else:
                self._log_pipeline_step("load_data", "error", {"error": "Database load failed"})
            
            return success
            
        except Exception as e:
            self._log_pipeline_step("load_data", "error", {"error": str(e)})
            raise
    
    def run_csv_pipeline(self, file_paths: List[str], table_name: str, 
                        transformation_config: Dict = None,
                        load_strategy: str = 'append') -> bool:
        try:
            self.logger.info(f"🚀 Starting CSV ETL pipeline for table: {table_name}")
            start_time = datetime.now()
            
            # Extract
            df = self.extract_csv_data(file_paths, table_name)
            
            # Transform
            transformed_df = self.transform_data(df, table_name, transformation_config)
            
            # Load
            success = self.load_data(transformed_df, table_name, load_strategy)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if success:
                self._log_pipeline_step("csv_pipeline", "success", {
                    "table": table_name,
                    "duration_seconds": duration,
                    "total_rows": len(transformed_df)
                })
                self.logger.info(f"✅ CSV pipeline completed successfully in {duration:.2f} seconds")
            else:
                self._log_pipeline_step("csv_pipeline", "error", {"error": "Pipeline failed"})
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ CSV pipeline failed: {str(e)}")
            return False
    
    def run_api_pipeline(self, endpoint: str, table_name: str, 
                        params: Dict = None,
                        transformation_config: Dict = None,
                        load_strategy: str = 'append') -> bool:
        try:
            self.logger.info(f"🚀 Starting API ETL pipeline for table: {table_name}")
            start_time = datetime.now()
            
            # Extract
            df = self.extract_api_data(endpoint, table_name, params)
            
            # Transform
            transformed_df = self.transform_data(df, table_name, transformation_config)
            
            # Load
            success = self.load_data(transformed_df, table_name, load_strategy)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if success:
                self._log_pipeline_step("api_pipeline", "success", {
                    "table": table_name,
                    "endpoint": endpoint,
                    "duration_seconds": duration,
                    "total_rows": len(transformed_df)
                })
                self.logger.info(f"✅ API pipeline completed successfully in {duration:.2f} seconds")
            else:
                self._log_pipeline_step("api_pipeline", "error", {"error": "Pipeline failed"})
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ API pipeline failed: {str(e)}")
            return False
    
    def run_full_pipeline(self, pipeline_config: Dict) -> Dict[str, bool]:
        results = {}
        
        self.logger.info(f"🚀 Starting full ETL pipeline: {self.pipeline_name}")
        pipeline_start = datetime.now()
        
        try:
            for job_name, job_config in pipeline_config.items():
                self.logger.info(f"📋 Processing job: {job_name}")
                
                job_type = job_config.get('type', 'csv')
                table_name = job_config.get('table_name')
                
                if job_type == 'csv':
                    file_paths = job_config.get('file_paths', [])
                    transformation_config = job_config.get('transformations', {})
                    load_strategy = job_config.get('load_strategy', 'append')
                    
                    success = self.run_csv_pipeline(
                        file_paths, table_name, transformation_config, load_strategy
                    )
                    
                elif job_type == 'api':
                    endpoint = job_config.get('endpoint')
                    params = job_config.get('params', {})
                    transformation_config = job_config.get('transformations', {})
                    load_strategy = job_config.get('load_strategy', 'append')
                    
                    success = self.run_api_pipeline(
                        endpoint, table_name, params, transformation_config, load_strategy
                    )
                
                else:
                    self.logger.error(f"❌ Unknown job type: {job_type}")
                    success = False
                
                results[job_name] = success
                
                if not success:
                    self.logger.error(f"❌ Job {job_name} failed, stopping pipeline")
                    break
        
        except Exception as e:
            self.logger.error(f"❌ Full pipeline failed: {str(e)}")
        
        finally:
            pipeline_end = datetime.now()
            pipeline_duration = (pipeline_end - pipeline_start).total_seconds()
            
            successful_jobs = sum(1 for success in results.values() if success)
            total_jobs = len(results)
            
            self.logger.info(f"📊 Pipeline Summary:")
            self.logger.info(f"   Total Jobs: {total_jobs}")
            self.logger.info(f"   Successful: {successful_jobs}")
            self.logger.info(f"   Failed: {total_jobs - successful_jobs}")
            self.logger.info(f"   Duration: {pipeline_duration:.2f} seconds")
            
            # Log pipeline completion to database
            self._log_pipeline_to_database(results, pipeline_duration)
        
        return results
    
    def _log_pipeline_to_database(self, results: Dict[str, bool], duration: float):
        try:
            log_data = {
                'job_name': self.pipeline_name,
                'job_type': 'full_pipeline',
                'start_time': self.pipeline_log[0]['timestamp'] if self.pipeline_log else datetime.now(),
                'end_time': datetime.now(),
                'status': 'success' if all(results.values()) else 'partial_success' if any(results.values()) else 'failed',
                'records_processed': sum(log['details'].get('rows_extracted', 0) for log in self.pipeline_log if 'rows_extracted' in log['details']),
                'records_loaded': sum(log['details'].get('rows_loaded', 0) for log in self.pipeline_log if 'rows_loaded' in log['details']),
                'metadata': {
                    'pipeline_results': results,
                    'duration_seconds': duration,
                    'total_steps': len(self.pipeline_log)
                }
            }
            
            # Create a DataFrame for the log entry
            log_df = pd.DataFrame([log_data])
            self.db_loader.load_dataframe(log_df, 'etl_job_logs', if_exists='append')
            
        except Exception as e:
            self.logger.warning(f"Failed to log pipeline to database: {str(e)}")
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        return {
            'pipeline_name': self.pipeline_name,
            'total_steps': len(self.pipeline_log),
            'pipeline_log': self.pipeline_log,
            'summary': {
                'successful_steps': sum(1 for log in self.pipeline_log if log['status'] == 'success'),
                'failed_steps': sum(1 for log in self.pipeline_log if log['status'] == 'error'),
                'start_time': self.pipeline_log[0]['timestamp'] if self.pipeline_log else None,
                'end_time': self.pipeline_log[-1]['timestamp'] if self.pipeline_log else None
            }
        }
    
    def cleanup(self):
        try:
            self.db_loader.close()
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":
    # Example usage
    pipeline = ETLPipeline("example_pipeline")
    
    # Example pipeline configuration
    example_config = {
        'users_job': {
            'type': 'csv',
            'table_name': 'raw_data.users',
            'file_paths': ['data/raw/users.csv'],
            'transformations': {
                'type_mapping': {
                    'registration_date': 'datetime',
                    'last_active': 'datetime'
                },
                'filters': {
                    'age': {'min': 18, 'max': 100}
                }
            },
            'load_strategy': 'replace'
        },
        'products_job': {
            'type': 'api',
            'table_name': 'raw_data.products',
            'endpoint': 'products',
            'transformations': {
                'type_mapping': {
                    'created_date': 'datetime',
                    'updated_date': 'datetime'
                }
            },
            'load_strategy': 'append'
        }
    }
    
    try:
        results = pipeline.run_full_pipeline(example_config)
        print(f"Pipeline results: {results}")
        print(pipeline.get_pipeline_summary())
    finally:
        pipeline.cleanup()
