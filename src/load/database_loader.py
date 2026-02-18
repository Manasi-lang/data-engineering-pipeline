import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.types import String, Integer, Float, DateTime, Boolean
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os

class DatabaseLoader:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.logger = self._setup_logger()
        self.load_log = []
    
    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _log_load(self, operation: str, table_name: str, rows_count: int, details: str = ""):
        log_entry = {
            'timestamp': datetime.now(),
            'operation': operation,
            'table_name': table_name,
            'rows_count': rows_count,
            'details': details
        }
        self.load_log.append(log_entry)
        self.logger.info(f"Load: {operation} - Table: {table_name} - Rows: {rows_count} {details}")
    
    def _infer_sql_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        type_mapping = {}
        
        for col in df.columns:
            dtype = df[col].dtype
            
            if pd.api.types.is_integer_dtype(dtype):
                type_mapping[col] = Integer()
            elif pd.api.types.is_float_dtype(dtype):
                type_mapping[col] = Float()
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                type_mapping[col] = DateTime()
            elif pd.api.types.is_bool_dtype(dtype):
                type_mapping[col] = Boolean()
            else:
                # For object types, use String with max length
                max_length = df[col].astype(str).str.len().max() if not df[col].empty else 255
                type_mapping[col] = String(max_length=max_length)
        
        return type_mapping
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str, 
                                   if_exists: str = 'replace', 
                                   primary_key: str = None) -> bool:
        try:
            # Infer SQL types from DataFrame
            dtype_mapping = self._infer_sql_types(df)
            
            # Create the table
            df.head(0).to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                dtype=dtype_mapping,
                index=False
            )
            
            # Add primary key if specified
            if primary_key and primary_key in df.columns:
                with self.engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key});"))
                    conn.commit()
            
            self._log_load('create_table', table_name, 0, f"if_exists={if_exists}, primary_key={primary_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating table {table_name}: {str(e)}")
            return False
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, 
                      if_exists: str = 'append', 
                      chunk_size: int = 1000,
                      create_table: bool = True) -> bool:
        try:
            rows_loaded = len(df)
            
            # Create table if it doesn't exist and create_table is True
            if create_table:
                inspector = inspect(self.engine)
                if not inspector.has_table(table_name):
                    self.create_table_from_dataframe(df, table_name, if_exists='fail')
            
            # Load data in chunks
            if chunk_size and len(df) > chunk_size:
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    chunk.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists=if_exists if i == 0 else 'append',
                        index=False,
                        method='multi'
                    )
            else:
                df.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists=if_exists,
                    index=False,
                    method='multi'
                )
            
            self._log_load('load_dataframe', table_name, rows_loaded, 
                         f"if_exists={if_exists}, chunk_size={chunk_size}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to {table_name}: {str(e)}")
            return False
    
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        conflict_columns: List[str],
                        update_columns: List[str] = None) -> bool:
        try:
            if not conflict_columns:
                raise ValueError("conflict_columns must be specified for upsert operation")
            
            # Create table if it doesn't exist
            inspector = inspect(self.engine)
            if not inspector.has_table(table_name):
                self.create_table_from_dataframe(df, table_name, if_exists='fail')
            
            # Get all columns if update_columns not specified
            if not update_columns:
                update_columns = [col for col in df.columns if col not in conflict_columns]
            
            # Build upsert query
            conflict_cols_str = ', '.join(conflict_columns)
            update_cols_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            # Load to temporary table first
            temp_table = f"{table_name}_temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            df.to_sql(name=temp_table, con=self.engine, if_exists='replace', index=False)
            
            # Perform upsert
            upsert_query = f"""
            INSERT INTO {table_name} ({', '.join(df.columns)})
            SELECT {', '.join(df.columns)}
            FROM {temp_table}
            ON CONFLICT ({conflict_cols_str}) DO UPDATE SET
                {update_cols_str};
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(upsert_query))
                conn.execute(text(f"DROP TABLE {temp_table};"))
                conn.commit()
            
            self._log_load('upsert_dataframe', table_name, len(df), 
                         f"conflict_columns={conflict_columns}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error upserting data to {table_name}: {str(e)}")
            return False
    
    def execute_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                self._log_load('execute_query', 'custom_query', len(df), f"query_length={len(query)}")
                return df
                
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        try:
            inspector = inspect(self.engine)
            
            if not inspector.has_table(table_name):
                return {'error': f'Table {table_name} does not exist'}
            
            # Get column information
            columns = inspector.get_columns(table_name)
            
            # Get row count
            row_count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            with self.engine.connect() as conn:
                result = conn.execute(text(row_count_query))
                row_count = result.fetchone()[0]
            
            # Get sample data
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            with self.engine.connect() as conn:
                result = conn.execute(text(sample_query))
                sample_data = [dict(row._mapping) for row in result]
            
            info = {
                'table_name': table_name,
                'columns': columns,
                'row_count': row_count,
                'sample_data': sample_data,
                'column_count': len(columns)
            }
            
            return info
            
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def backup_table(self, table_name: str, backup_suffix: str = None) -> bool:
        try:
            if not backup_suffix:
                backup_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            backup_table = f"{table_name}_backup_{backup_suffix}"
            
            # Create backup table
            backup_query = f"CREATE TABLE {backup_table} AS SELECT * FROM {table_name};"
            
            with self.engine.connect() as conn:
                conn.execute(text(backup_query))
                conn.commit()
            
            self._log_load('backup_table', backup_table, 0, f"original_table={table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error backing up table {table_name}: {str(e)}")
            return False
    
    def get_load_summary(self) -> Dict[str, Any]:
        return {
            'total_loads': len(self.load_log),
            'load_log': self.load_log,
            'summary': {
                'total_rows_loaded': sum(log['rows_count'] for log in self.load_log),
                'tables_loaded': list(set(log['table_name'] for log in self.load_log)),
                'operations': list(set(log['operation'] for log in self.load_log))
            }
        }
    
    def close(self):
        if hasattr(self, 'engine'):
            self.engine.dispose()

if __name__ == "__main__":
    # Example usage
    from config.config import config
    
    loader = DatabaseLoader(config.DATABASE_URL)
    
    # Create sample data
    sample_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000, 60000, 70000, 80000, 90000],
        'department': ['IT', 'HR', 'Finance', 'IT', 'Marketing'],
        'hire_date': pd.to_datetime(['2020-01-01', '2019-05-15', '2018-11-20', '2021-03-10', '2017-07-25'])
    })
    
    try:
        # Load data
        success = loader.load_dataframe(sample_data, 'employees', if_exists='replace')
        print(f"Load successful: {success}")
        
        # Get table info
        info = loader.get_table_info('employees')
        print(f"Table info: {info}")
        
        # Get load summary
        summary = loader.get_load_summary()
        print(f"Load summary: {summary}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        loader.close()
