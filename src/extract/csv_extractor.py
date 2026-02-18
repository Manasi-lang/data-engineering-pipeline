import pandas as pd
import os
import logging
from typing import List, Dict, Any
from datetime import datetime

class CSVExtractor:
    def __init__(self, data_path: str = None):
        self.data_path = data_path or 'data/raw'
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def extract_from_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        try:
            self.logger.info(f"Extracting data from CSV: {file_path}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")
            
            df = pd.read_csv(file_path, **kwargs)
            self.logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting from CSV {file_path}: {str(e)}")
            raise
    
    def extract_multiple_csv(self, file_patterns: List[str], combine: bool = True) -> pd.DataFrame:
        dataframes = []
        
        for pattern in file_patterns:
            try:
                df = self.extract_from_csv(pattern)
                df['source_file'] = os.path.basename(pattern)
                df['extraction_timestamp'] = datetime.now()
                dataframes.append(df)
            except Exception as e:
                self.logger.warning(f"Failed to extract {pattern}: {str(e)}")
                continue
        
        if not dataframes:
            raise ValueError("No CSV files were successfully extracted")
        
        if combine:
            combined_df = pd.concat(dataframes, ignore_index=True)
            self.logger.info(f"Combined {len(dataframes)} CSV files into {len(combined_df)} rows")
            return combined_df
        else:
            return dataframes
    
    def get_csv_info(self, file_path: str) -> Dict[str, Any]:
        try:
            df = pd.read_csv(file_path, nrows=5)
            full_df = pd.read_csv(file_path)
            
            info = {
                'file_path': file_path,
                'file_size': os.path.getsize(file_path),
                'total_rows': len(full_df),
                'total_columns': len(df.columns),
                'columns': list(df.columns),
                'data_types': df.dtypes.to_dict(),
                'sample_data': df.head().to_dict('records')
            }
            
            return info
            
        except Exception as e:
            self.logger.error(f"Error getting CSV info for {file_path}: {str(e)}")
            raise

if __name__ == "__main__":
    extractor = CSVExtractor()
    
    # Example usage
    try:
        df = extractor.extract_from_csv('data/raw/sample_data.csv')
        print(f"Extracted {len(df)} rows")
        print(df.head())
    except Exception as e:
        print(f"Error: {e}")
