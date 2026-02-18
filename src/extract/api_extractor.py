import requests
import pandas as pd
import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

class APIExtractor:
    def __init__(self, base_url: str, api_key: str = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.logger = self._setup_logger()
        
        if api_key:
            self.session.headers.update({'Authorization': f'Bearer {api_key}'})
    
    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _make_request(self, endpoint: str, params: Dict = None, retries: int = 3) -> Dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                self.logger.info(f"Successfully fetched data from {url}")
                return response.json()
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
    
    def extract_to_dataframe(self, endpoint: str, params: Dict = None, 
                           data_key: str = None, **kwargs) -> pd.DataFrame:
        try:
            self.logger.info(f"Extracting data from API endpoint: {endpoint}")
            
            data = self._make_request(endpoint, params)
            
            if data_key:
                data = data.get(data_key, data)
            
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.json_normalize(data)
            else:
                raise ValueError(f"Unsupported data format: {type(data)}")
            
            df['extraction_timestamp'] = datetime.now()
            df['source_endpoint'] = endpoint
            
            self.logger.info(f"Successfully extracted {len(df)} records from API")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting from API endpoint {endpoint}: {str(e)}")
            raise
    
    def extract_paginated_data(self, endpoint: str, params: Dict = None,
                              page_param: str = 'page', size_param: str = 'limit',
                              max_pages: int = None) -> pd.DataFrame:
        all_data = []
        page = 1
        
        if params is None:
            params = {}
        
        while True:
            if max_pages and page > max_pages:
                break
                
            current_params = params.copy()
            current_params[page_param] = page
            
            try:
                response_data = self._make_request(endpoint, current_params)
                
                if isinstance(response_data, dict):
                    data = response_data.get('data', response_data.get('results', []))
                    pagination = response_data.get('pagination', response_data.get('meta', {}))
                else:
                    data = response_data
                    pagination = {}
                
                if not data:
                    break
                
                all_data.extend(data)
                
                total_pages = pagination.get('total_pages')
                if total_pages and page >= total_pages:
                    break
                
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                break
        
        if not all_data:
            raise ValueError("No data extracted from paginated API")
        
        df = pd.DataFrame(all_data)
        df['extraction_timestamp'] = datetime.now()
        df['source_endpoint'] = endpoint
        
        self.logger.info(f"Extracted {len(df)} records from {len(all_data)} pages")
        return df
    
    def get_api_info(self, endpoint: str) -> Dict[str, Any]:
        try:
            response = self.session.get(f"{self.base_url}/{endpoint.lstrip('/')}", timeout=10)
            
            info = {
                'url': f"{self.base_url}/{endpoint.lstrip('/')}",
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'content_type': response.headers.get('content-type'),
                'response_size': len(response.content)
            }
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, list):
                        info['data_structure'] = 'list'
                        info['sample_record'] = data[0] if data else None
                    elif isinstance(data, dict):
                        info['data_structure'] = 'dict'
                        info['keys'] = list(data.keys())
                except:
                    info['data_structure'] = 'non-json'
            
            return info
            
        except Exception as e:
            self.logger.error(f"Error getting API info for {endpoint}: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    extractor = APIExtractor("https://jsonplaceholder.typicode.com")
    
    try:
        df = extractor.extract_to_dataframe("users")
        print(f"Extracted {len(df)} users")
        print(df.head())
    except Exception as e:
        print(f"Error: {e}")
