import boto3
import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import os
import json
from botocore.exceptions import ClientError, NoCredentialsError

class S3Integration:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, 
                 aws_region: str = 'us-east-1', bucket_name: str = None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = aws_region
        self.bucket_name = bucket_name
        
        self.logger = self._setup_logger()
        self.s3_client = None
        self.s3_resource = None
        
        self._initialize_s3()
    
    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _initialize_s3(self):
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region
            )
            
            self.s3_resource = boto3.resource(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region
            )
            
            # Test connection
            self.s3_client.list_buckets()
            self.logger.info("✅ S3 connection initialized successfully")
            
        except NoCredentialsError:
            self.logger.error("❌ AWS credentials not found")
            raise
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize S3: {str(e)}")
            raise
    
    def create_bucket(self, bucket_name: str = None) -> bool:
        try:
            bucket = bucket_name or self.bucket_name
            
            if not bucket:
                raise ValueError("Bucket name is required")
            
            # Check if bucket already exists
            try:
                self.s3_client.head_bucket(Bucket=bucket)
                self.logger.info(f"Bucket {bucket} already exists")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise
            
            # Create bucket
            if self.aws_region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=bucket)
            else:
                self.s3_client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                )
            
            self.logger.info(f"✅ Bucket {bucket} created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create bucket: {str(e)}")
            return False
    
    def upload_dataframe_to_s3(self, df: pd.DataFrame, key: str, 
                              bucket_name: str = None, 
                              file_format: str = 'csv',
                              index: bool = False) -> bool:
        try:
            bucket = bucket_name or self.bucket_name
            
            if not bucket:
                raise ValueError("Bucket name is required")
            
            # Convert DataFrame to appropriate format
            if file_format.lower() == 'csv':
                csv_buffer = df.to_csv(index=index)
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=csv_buffer
                )
            elif file_format.lower() == 'json':
                json_buffer = df.to_json(orient='records', lines=True)
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=json_buffer
                )
            elif file_format.lower() == 'parquet':
                parquet_buffer = df.to_parquet(index=index)
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=parquet_buffer
                )
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            self.logger.info(f"✅ DataFrame uploaded to s3://{bucket}/{key}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to upload DataFrame to S3: {str(e)}")
            return False
    
    def upload_file_to_s3(self, file_path: str, key: str, 
                         bucket_name: str = None) -> bool:
        try:
            bucket = bucket_name or self.bucket_name
            
            if not bucket:
                raise ValueError("Bucket name is required")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            self.s3_client.upload_file(file_path, bucket, key)
            
            self.logger.info(f"✅ File uploaded to s3://{bucket}/{key}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to upload file to S3: {str(e)}")
            return False
    
    def download_from_s3(self, key: str, file_path: str, 
                        bucket_name: str = None) -> bool:
        try:
            bucket = bucket_name or self.bucket_name
            
            if not bucket:
                raise ValueError("Bucket name is required")
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            self.s3_client.download_file(bucket, key, file_path)
            
            self.logger.info(f"✅ File downloaded from s3://{bucket}/{key} to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to download from S3: {str(e)}")
            return False
    
    def read_s3_to_dataframe(self, key: str, bucket_name: str = None, 
                           file_format: str = 'csv', **kwargs) -> pd.DataFrame:
        try:
            bucket = bucket_name or self.bucket_name
            
            if not bucket:
                raise ValueError("Bucket name is required")
            
            # Get object from S3
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            body = obj['Body']
            
            # Read based on file format
            if file_format.lower() == 'csv':
                df = pd.read_csv(body, **kwargs)
            elif file_format.lower() == 'json':
                df = pd.read_json(body, lines=True, **kwargs)
            elif file_format.lower() == 'parquet':
                df = pd.read_parquet(body, **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            self.logger.info(f"✅ DataFrame loaded from s3://{bucket}/{key}")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Failed to read S3 to DataFrame: {str(e)}")
            raise
    
    def list_s3_objects(self, prefix: str = '', bucket_name: str = None) -> List[Dict]:
        try:
            bucket = bucket_name or self.bucket_name
            
            if not bucket:
                raise ValueError("Bucket name is required")
            
            objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag'].strip('"'),
                            'storage_class': obj.get('StorageClass', 'STANDARD')
                        })
            
            self.logger.info(f"✅ Found {len(objects)} objects in s3://{bucket}/{prefix}")
            return objects
            
        except Exception as e:
            self.logger.error(f"❌ Failed to list S3 objects: {str(e)}")
            return []
    
    def delete_s3_object(self, key: str, bucket_name: str = None) -> bool:
        try:
            bucket = bucket_name or self.bucket_name
            
            if not bucket:
                raise ValueError("Bucket name is required")
            
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            
            self.logger.info(f"✅ Object deleted: s3://{bucket}/{key}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to delete S3 object: {str(e)}")
            return False
    
    def copy_s3_object(self, source_key: str, dest_key: str, 
                      source_bucket: str = None, dest_bucket: str = None) -> bool:
        try:
            source_bucket = source_bucket or self.bucket_name
            dest_bucket = dest_bucket or self.bucket_name
            
            if not source_bucket or not dest_bucket:
                raise ValueError("Bucket names are required")
            
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
            
            self.logger.info(f"✅ Object copied from s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to copy S3 object: {str(e)}")
            return False
    
    def get_s3_object_info(self, key: str, bucket_name: str = None) -> Dict[str, Any]:
        try:
            bucket = bucket_name or self.bucket_name
            
            if not bucket:
                raise ValueError("Bucket name is required")
            
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            
            info = {
                'key': key,
                'bucket': bucket,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType'),
                'etag': response.get('ETag', '').strip('"'),
                'metadata': response.get('Metadata', {}),
                'storage_class': response.get('StorageClass', 'STANDARD')
            }
            
            return info
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get S3 object info: {str(e)}")
            return {}
    
    def backup_data_to_s3(self, data: pd.DataFrame, table_name: str, 
                         backup_date: datetime = None) -> bool:
        try:
            if not backup_date:
                backup_date = datetime.now()
            
            # Create backup key with date
            backup_key = f"backups/{table_name}/{backup_date.strftime('%Y/%m/%d')}/{table_name}_{backup_date.strftime('%Y%m%d_%H%M%S')}.csv"
            
            success = self.upload_dataframe_to_s3(data, backup_key)
            
            if success:
                self.logger.info(f"✅ Backup created for {table_name}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Failed to backup data to S3: {str(e)}")
            return False
    
    def archive_old_data(self, days_old: int = 30, source_prefix: str = 'data/raw/', 
                        archive_prefix: str = 'archive/') -> bool:
        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            # List objects in source prefix
            objects = self.list_s3_objects(source_prefix)
            
            archived_count = 0
            
            for obj in objects:
                if obj['last_modified'] < cutoff_date:
                    source_key = obj['key']
                    archive_key = source_key.replace(source_prefix, archive_prefix)
                    
                    # Copy to archive
                    if self.copy_s3_object(source_key, archive_key):
                        # Delete original
                        if self.delete_s3_object(source_key):
                            archived_count += 1
            
            self.logger.info(f"✅ Archived {archived_count} objects older than {days_old} days")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to archive old data: {str(e)}")
            return False

if __name__ == "__main__":
    # Example usage
    from config.config import config
    
    s3_integration = S3Integration(
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        aws_region=config.AWS_REGION,
        bucket_name=config.S3_BUCKET_NAME
    )
    
    # Create sample data
    sample_data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    })
    
    try:
        # Create bucket
        s3_integration.create_bucket()
        
        # Upload data
        s3_integration.upload_dataframe_to_s3(sample_data, 'test/sample_data.csv')
        
        # List objects
        objects = s3_integration.list_s3_objects()
        print(f"Objects in bucket: {len(objects)}")
        
        # Download data
        s3_integration.download_from_s3('test/sample_data.csv', 'data/downloaded_data.csv')
        
        # Read back to DataFrame
        downloaded_df = s3_integration.read_s3_to_dataframe('test/sample_data.csv')
        print(f"Downloaded DataFrame shape: {downloaded_df.shape}")
        
    except Exception as e:
        print(f"Error: {e}")
