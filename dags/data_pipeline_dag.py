from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.etl_pipeline import ETLPipeline
from config.config import config

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Pipeline configuration - can be moved to Airflow Variables for production
PIPELINE_CONFIG = {
    'users_etl': {
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
    'products_etl': {
        'type': 'csv',
        'table_name': 'raw_data.products',
        'file_paths': ['data/raw/products.csv'],
        'transformations': {
            'type_mapping': {
                'created_date': 'datetime',
                'updated_date': 'datetime'
            }
        },
        'load_strategy': 'append'
    },
    'sales_etl': {
        'type': 'csv',
        'table_name': 'raw_data.sales',
        'file_paths': ['data/raw/sales.csv'],
        'transformations': {
            'type_mapping': {
                'sale_date': 'datetime'
            },
            'filters': {
                'total_amount': {'min': 0}
            }
        },
        'load_strategy': 'append'
    }
}

def run_etl_job(**context):
    """Run a specific ETL job"""
    job_name = context['task_instance'].task_id
    job_config = PIPELINE_CONFIG.get(job_name.replace('_etl', ''))
    
    if not job_config:
        raise ValueError(f"No configuration found for job: {job_name}")
    
    pipeline = ETLPipeline(f"airflow_{job_name}")
    
    try:
        if job_config['type'] == 'csv':
            success = pipeline.run_csv_pipeline(
                file_paths=job_config['file_paths'],
                table_name=job_config['table_name'],
                transformation_config=job_config.get('transformations', {}),
                load_strategy=job_config.get('load_strategy', 'append')
            )
        elif job_config['type'] == 'api':
            success = pipeline.run_api_pipeline(
                endpoint=job_config['endpoint'],
                table_name=job_config['table_name'],
                params=job_config.get('params', {}),
                transformation_config=job_config.get('transformations', {}),
                load_strategy=job_config.get('load_strategy', 'append')
            )
        else:
            raise ValueError(f"Unsupported job type: {job_config['type']}")
        
        if not success:
            raise Exception(f"ETL job {job_name} failed")
        
        return f"ETL job {job_name} completed successfully"
        
    finally:
        pipeline.cleanup()

def run_analytics_pipeline(**context):
    """Run analytics and aggregation pipeline"""
    pipeline = ETLPipeline("airflow_analytics")
    
    try:
        # Get database connection
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Run user analytics
        user_analytics_query = """
        INSERT INTO processed_data.user_analytics (
            user_id, name, age_group, location, total_purchases, total_spent,
            avg_purchase_value, favorite_category, customer_lifetime_days,
            last_purchase_date, first_purchase_date, is_active
        )
        SELECT 
            u.user_id,
            u.name,
            CASE 
                WHEN u.age < 25 THEN '18-24'
                WHEN u.age < 35 THEN '25-34'
                WHEN u.age < 45 THEN '35-44'
                WHEN u.age < 55 THEN '45-54'
                ELSE '55+'
            END as age_group,
            u.location,
            COUNT(s.sale_id) as total_purchases,
            COALESCE(SUM(s.total_amount), 0) as total_spent,
            CASE 
                WHEN COUNT(s.sale_id) > 0 THEN COALESCE(SUM(s.total_amount), 0) / COUNT(s.sale_id)
                ELSE 0
            END as avg_purchase_value,
            (
                SELECT p.category
                FROM raw_data.sales s2
                JOIN raw_data.products p ON s2.product_id = p.product_id
                WHERE s2.user_id = u.user_id
                GROUP BY p.category
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) as favorite_category,
            CASE 
                WHEN u.last_active IS NOT NULL THEN 
                    (CURRENT_DATE - u.registration_date)
                ELSE NULL
            END as customer_lifetime_days,
            MAX(s.sale_date) as last_purchase_date,
            MIN(s.sale_date) as first_purchase_date,
            CASE 
                WHEN u.last_active >= CURRENT_DATE - INTERVAL '30 days' THEN TRUE
                ELSE FALSE
            END as is_active
        FROM raw_data.users u
        LEFT JOIN raw_data.sales s ON u.user_id = s.user_id
        GROUP BY u.user_id, u.name, u.age, u.location, u.registration_date, u.last_active
        ON CONFLICT (user_id) DO UPDATE SET
            name = EXCLUDED.name,
            age_group = EXCLUDED.age_group,
            location = EXCLUDED.location,
            total_purchases = EXCLUDED.total_purchases,
            total_spent = EXCLUDED.total_spent,
            avg_purchase_value = EXCLUDED.avg_purchase_value,
            favorite_category = EXCLUDED.favorite_category,
            customer_lifetime_days = EXCLUDED.customer_lifetime_days,
            last_purchase_date = EXCLUDED.last_purchase_date,
            first_purchase_date = EXCLUDED.first_purchase_date,
            is_active = EXCLUDED.is_active,
            processing_date = CURRENT_TIMESTAMP;
        """
        
        postgres_hook.run(user_analytics_query)
        
        # Run product analytics
        product_analytics_query = """
        INSERT INTO processed_data.product_analytics (
            product_id, name, category, brand, total_sales, total_revenue,
            avg_price, unique_customers, top_locations
        )
        SELECT 
            p.product_id,
            p.name,
            p.category,
            p.brand,
            COUNT(s.sale_id) as total_sales,
            COALESCE(SUM(s.total_amount), 0) as total_revenue,
            AVG(p.price) as avg_price,
            COUNT(DISTINCT s.user_id) as unique_customers,
            ARRAY_AGG(DISTINCT s.store_location) as top_locations
        FROM raw_data.products p
        LEFT JOIN raw_data.sales s ON p.product_id = s.product_id
        GROUP BY p.product_id, p.name, p.category, p.brand, p.price
        ON CONFLICT (product_id) DO UPDATE SET
            name = EXCLUDED.name,
            category = EXCLUDED.category,
            brand = EXCLUDED.brand,
            total_sales = EXCLUDED.total_sales,
            total_revenue = EXCLUDED.total_revenue,
            avg_price = EXCLUDED.avg_price,
            unique_customers = EXCLUDED.unique_customers,
            top_locations = EXCLUDED.top_locations,
            processing_date = CURRENT_TIMESTAMP;
        """
        
        postgres_hook.run(product_analytics_query)
        
        # Run sales analytics
        sales_analytics_query = """
        INSERT INTO processed_data.sales_analytics (
            date, total_sales, total_revenue, avg_order_value,
            unique_customers, unique_products, top_category, top_location
        )
        SELECT 
            s.sale_date as date,
            COUNT(s.sale_id) as total_sales,
            SUM(s.total_amount) as total_revenue,
            AVG(s.total_amount) as avg_order_value,
            COUNT(DISTINCT s.user_id) as unique_customers,
            COUNT(DISTINCT s.product_id) as unique_products,
            (
                SELECT p.category
                FROM raw_data.sales s2
                JOIN raw_data.products p ON s2.product_id = p.product_id
                WHERE s2.sale_date = s.sale_date
                GROUP BY p.category
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) as top_category,
            (
                SELECT s3.store_location
                FROM raw_data.sales s3
                WHERE s3.sale_date = s.sale_date
                GROUP BY s3.store_location
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) as top_location
        FROM raw_data.sales s
        GROUP BY s.sale_date
        ON CONFLICT (date) DO UPDATE SET
            total_sales = EXCLUDED.total_sales,
            total_revenue = EXCLUDED.total_revenue,
            avg_order_value = EXCLUDED.avg_order_value,
            unique_customers = EXCLUDED.unique_customers,
            unique_products = EXCLUDED.unique_products,
            top_category = EXCLUDED.top_category,
            top_location = EXCLUDED.top_location,
            processing_date = CURRENT_TIMESTAMP;
        """
        
        postgres_hook.run(sales_analytics_query)
        
        return "Analytics pipeline completed successfully"
        
    except Exception as e:
        raise Exception(f"Analytics pipeline failed: {str(e)}")

def data_quality_check(**context):
    """Run data quality checks"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    quality_checks = [
        {
            'table': 'raw_data.users',
            'check': 'SELECT COUNT(*) as total_rows FROM raw_data.users',
            'threshold': 0,
            'metric_name': 'total_users'
        },
        {
            'table': 'raw_data.products',
            'check': 'SELECT COUNT(*) as total_rows FROM raw_data.products',
            'threshold': 0,
            'metric_name': 'total_products'
        },
        {
            'table': 'raw_data.sales',
            'check': 'SELECT COUNT(*) as total_rows FROM raw_data.sales',
            'threshold': 0,
            'metric_name': 'total_sales'
        },
        {
            'table': 'raw_data.sales',
            'check': 'SELECT COUNT(*) as null_amounts FROM raw_data.sales WHERE total_amount IS NULL OR total_amount <= 0',
            'threshold': 0,
            'metric_name': 'invalid_sale_amounts'
        }
    ]
    
    failed_checks = []
    
    for check in quality_checks:
        result = postgres_hook.get_first(check['check'])[0]
        status = 'pass' if result >= check['threshold'] else 'fail'
        
        if status == 'fail':
            failed_checks.append(f"{check['metric_name']}: {result} (threshold: {check['threshold']})")
        
        # Log the metric
        metric_query = """
        INSERT INTO analytics.data_quality_metrics 
        (table_name, metric_name, metric_value, threshold_value, status, details)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        postgres_hook.run(metric_query, parameters=[
            check['table'],
            check['metric_name'],
            float(result),
            float(check['threshold']),
            status,
            {'check_query': check['check']}
        ])
    
    if failed_checks:
        raise Exception(f"Data quality checks failed: {'; '.join(failed_checks)}")
    
    return "All data quality checks passed"

# Create DAG
dag = DAG(
    'data_engineering_pipeline',
    default_args=default_args,
    description='End-to-End Data Engineering Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-engineering', 'etl'],
)

# Tasks
init_database = PostgresOperator(
    task_id='init_database',
    postgres_conn_id='postgres_default',
    sql='scripts/init_database.sql',
    dag=dag,
)

# ETL tasks
users_etl = PythonOperator(
    task_id='users_etl',
    python_callable=run_etl_job,
    dag=dag,
)

products_etl = PythonOperator(
    task_id='products_etl',
    python_callable=run_etl_job,
    dag=dag,
)

sales_etl = PythonOperator(
    task_id='sales_etl',
    python_callable=run_etl_job,
    dag=dag,
)

# Analytics task
analytics_pipeline = PythonOperator(
    task_id='analytics_pipeline',
    python_callable=run_analytics_pipeline,
    dag=dag,
)

# Data quality check
data_quality = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# Cleanup task
cleanup = BashOperator(
    task_id='cleanup',
    bash_command='echo "Pipeline cleanup completed"',
    dag=dag,
)

# Define task dependencies
init_database >> [users_etl, products_etl, sales_etl]
[users_etl, products_etl, sales_etl] >> analytics_pipeline
analytics_pipeline >> data_quality
data_quality >> cleanup
