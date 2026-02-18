import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import re

class DataTransformer:
    def __init__(self):
        self.logger = self._setup_logger()
        self.transformation_log = []
    
    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _log_transformation(self, operation: str, input_shape: Tuple, output_shape: Tuple, details: str = ""):
        log_entry = {
            'timestamp': datetime.now(),
            'operation': operation,
            'input_shape': input_shape,
            'output_shape': output_shape,
            'details': details
        }
        self.transformation_log.append(log_entry)
        self.logger.info(f"Transformation: {operation} - {input_shape} -> {output_shape} {details}")
    
    def clean_data(self, df: pd.DataFrame, 
                   remove_duplicates: bool = True,
                   handle_missing: str = 'drop',
                   missing_threshold: float = 0.5) -> pd.DataFrame:
        original_shape = df.shape
        
        cleaned_df = df.copy()
        
        # Remove duplicates
        if remove_duplicates:
            cleaned_df = cleaned_df.drop_duplicates()
        
        # Handle missing values
        if handle_missing == 'drop':
            # Drop columns with too many missing values
            missing_ratio = cleaned_df.isnull().sum() / len(cleaned_df)
            cols_to_keep = missing_ratio[missing_ratio <= missing_threshold].index
            cleaned_df = cleaned_df[cols_to_keep]
            
            # Drop rows with any missing values
            cleaned_df = cleaned_df.dropna()
        elif handle_missing == 'fill':
            # Fill numeric columns with median
            numeric_cols = cleaned_df.select_dtypes(include=[np.number]).columns
            cleaned_df[numeric_cols] = cleaned_df[numeric_cols].fillna(cleaned_df[numeric_cols].median())
            
            # Fill categorical columns with mode
            categorical_cols = cleaned_df.select_dtypes(include=['object']).columns
            for col in categorical_cols:
                mode_val = cleaned_df[col].mode()
                if len(mode_val) > 0:
                    cleaned_df[col] = cleaned_df[col].fillna(mode_val[0])
        
        self._log_transformation('clean_data', original_shape, cleaned_df.shape, 
                               f"missing_threshold={missing_threshold}, handle_missing={handle_missing}")
        return cleaned_df
    
    def standardize_columns(self, df: pd.DataFrame, 
                           naming_convention: str = 'snake_case') -> pd.DataFrame:
        original_shape = df.shape
        
        standardized_df = df.copy()
        
        if naming_convention == 'snake_case':
            # Convert to snake_case
            standardized_df.columns = [
                re.sub(r'[^a-zA-Z0-9_]', '_', col).lower().strip('_')
                for col in standardized_df.columns
            ]
        elif naming_convention == 'lower':
            standardized_df.columns = [col.lower().strip() for col in standardized_df.columns]
        elif naming_convention == 'upper':
            standardized_df.columns = [col.upper().strip() for col in standardized_df.columns]
        
        self._log_transformation('standardize_columns', original_shape, standardized_df.shape,
                               f"naming_convention={naming_convention}")
        return standardized_df
    
    def convert_data_types(self, df: pd.DataFrame, 
                          type_mapping: Dict[str, str] = None) -> pd.DataFrame:
        original_shape = df.shape
        
        converted_df = df.copy()
        
        if type_mapping:
            for col, dtype in type_mapping.items():
                if col in converted_df.columns:
                    try:
                        if dtype == 'datetime':
                            converted_df[col] = pd.to_datetime(converted_df[col])
                        elif dtype == 'category':
                            converted_df[col] = converted_df[col].astype('category')
                        else:
                            converted_df[col] = converted_df[col].astype(dtype)
                    except Exception as e:
                        self.logger.warning(f"Failed to convert {col} to {dtype}: {str(e)}")
        
        self._log_transformation('convert_data_types', original_shape, converted_df.shape,
                               f"type_mapping={type_mapping}")
        return converted_df
    
    def filter_data(self, df: pd.DataFrame, 
                   filters: Dict[str, Any]) -> pd.DataFrame:
        original_shape = df.shape
        
        filtered_df = df.copy()
        
        for column, condition in filters.items():
            if column not in filtered_df.columns:
                continue
                
            if isinstance(condition, dict):
                if 'min' in condition:
                    filtered_df = filtered_df[filtered_df[column] >= condition['min']]
                if 'max' in condition:
                    filtered_df = filtered_df[filtered_df[column] <= condition['max']]
                if 'values' in condition:
                    filtered_df = filtered_df[filtered_df[column].isin(condition['values'])]
                if 'not_values' in condition:
                    filtered_df = filtered_df[~filtered_df[column].isin(condition['not_values'])]
            else:
                filtered_df = filtered_df[filtered_df[column] == condition]
        
        self._log_transformation('filter_data', original_shape, filtered_df.shape,
                               f"filters={filters}")
        return filtered_df
    
    def create_features(self, df: pd.DataFrame, 
                       feature_config: Dict[str, Dict]) -> pd.DataFrame:
        original_shape = df.shape
        
        enhanced_df = df.copy()
        
        for feature_name, config in feature_config.items():
            feature_type = config.get('type')
            
            if feature_type == 'derived':
                # Create derived features from existing columns
                expression = config.get('expression')
                if expression:
                    try:
                        enhanced_df[feature_name] = enhanced_df.eval(expression)
                    except Exception as e:
                        self.logger.warning(f"Failed to create feature {feature_name}: {str(e)}")
            
            elif feature_type == 'date_features':
                # Extract date features from datetime columns
                date_col = config.get('column')
                if date_col and date_col in enhanced_df.columns:
                    enhanced_df[feature_name + '_year'] = enhanced_df[date_col].dt.year
                    enhanced_df[feature_name + '_month'] = enhanced_df[date_col].dt.month
                    enhanced_df[feature_name + '_day'] = enhanced_df[date_col].dt.day
                    enhanced_df[feature_name + '_weekday'] = enhanced_df[date_col].dt.weekday
            
            elif feature_type == 'categorical_encoding':
                # One-hot encode categorical columns
                col = config.get('column')
                if col and col in enhanced_df.columns:
                    dummies = pd.get_dummies(enhanced_df[col], prefix=feature_name)
                    enhanced_df = pd.concat([enhanced_df, dummies], axis=1)
        
        self._log_transformation('create_features', original_shape, enhanced_df.shape,
                               f"features_created={list(feature_config.keys())}")
        return enhanced_df
    
    def aggregate_data(self, df: pd.DataFrame, 
                      group_by: List[str], 
                      aggregations: Dict[str, List[str]]) -> pd.DataFrame:
        original_shape = df.shape
        
        try:
            aggregated_df = df.groupby(group_by).agg(aggregations).reset_index()
            
            # Flatten multi-level column names
            aggregated_df.columns = ['_'.join(col).strip() if col[1] else col[0] 
                                   for col in aggregated_df.columns.values]
            
            self._log_transformation('aggregate_data', original_shape, aggregated_df.shape,
                                   f"group_by={group_by}, aggregations={aggregations}")
            return aggregated_df
            
        except Exception as e:
            self.logger.error(f"Error in aggregation: {str(e)}")
            raise
    
    def validate_data(self, df: pd.DataFrame, 
                     validation_rules: Dict[str, Dict]) -> Tuple[pd.DataFrame, Dict]:
        validation_results = {}
        validated_df = df.copy()
        
        for column, rules in validation_rules.items():
            if column not in df.columns:
                continue
                
            column_results = {}
            
            # Check for null values
            if 'not_null' in rules and rules['not_null']:
                null_count = df[column].isnull().sum()
                column_results['null_check'] = {
                    'passed': null_count == 0,
                    'null_count': null_count,
                    'null_percentage': (null_count / len(df)) * 100
                }
            
            # Check value range
            if 'range' in rules:
                min_val, max_val = rules['range']
                out_of_range = ((df[column] < min_val) | (df[column] > max_val)).sum()
                column_results['range_check'] = {
                    'passed': out_of_range == 0,
                    'out_of_range_count': out_of_range,
                    'range': [min_val, max_val]
                }
            
            # Check allowed values
            if 'allowed_values' in rules:
                invalid_values = ~df[column].isin(rules['allowed_values'])
                invalid_count = invalid_values.sum()
                column_results['allowed_values_check'] = {
                    'passed': invalid_count == 0,
                    'invalid_count': invalid_count,
                    'invalid_values': df[column][invalid_values].unique().tolist() if invalid_count > 0 else []
                }
            
            validation_results[column] = column_results
        
        return validated_df, validation_results
    
    def get_transformation_summary(self) -> Dict[str, Any]:
        return {
            'total_transformations': len(self.transformation_log),
            'transformation_log': self.transformation_log,
            'summary': {
                'initial_shape': self.transformation_log[0]['input_shape'] if self.transformation_log else None,
                'final_shape': self.transformation_log[-1]['output_shape'] if self.transformation_log else None,
                'total_rows_processed': sum(log['input_shape'][0] for log in self.transformation_log),
                'total_rows_output': sum(log['output_shape'][0] for log in self.transformation_log)
            }
        }

if __name__ == "__main__":
    # Example usage
    transformer = DataTransformer()
    
    # Create sample data
    sample_data = pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie', 'Alice', None],
        'Age': [25, 30, 35, 25, 40],
        'Salary': [50000, 60000, 70000, 50000, 80000],
        'Join Date': ['2020-01-01', '2019-05-15', '2018-11-20', '2020-01-01', '2017-03-10']
    })
    
    print("Original data:")
    print(sample_data)
    
    # Apply transformations
    cleaned_data = transformer.clean_data(sample_data)
    standardized_data = transformer.standardize_columns(cleaned_data)
    converted_data = transformer.convert_data_types(standardized_data, {
        'join_date': 'datetime'
    })
    
    print("\nTransformed data:")
    print(converted_data)
    
    print("\nTransformation summary:")
    print(transformer.get_transformation_summary())
