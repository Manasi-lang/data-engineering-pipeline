# 🚀 End-to-End Data Engineering Pipeline

A comprehensive, production-ready data engineering pipeline that demonstrates best practices for extracting, transforming, and loading data using modern tools and technologies.

## 🌐 **Live Dashboard**
📊 **Interactive Analytics Dashboard**: [View Live Demo](https://manasi-lang.github.io/data-pipeline-dashboard/)
- Real-time metrics and visualizations
- Interactive charts and data insights
- Mobile-responsive design
- Production-ready deployment

## 📋 **Quick Links**
- 🔗 **GitHub Repository**: https://github.com/manasi-lang/data-engineering-pipeline
- 🌐 **Live Dashboard**: https://manasi-lang.github.io/data-pipeline-dashboard/
- 📊 **Dashboard Source**: https://github.com/manasi-lang/data-engineering-pipeline/blob/main/Cloud_Dashboard/index.html

## 📋 Project Overview

This project implements a complete data engineering pipeline with the following capabilities:

- **Data Extraction**: CSV files and REST APIs
- **Data Transformation**: Advanced data cleaning and feature engineering with Pandas
- **Data Loading**: PostgreSQL database with optimized schemas
- **Orchestration**: Apache Airflow for workflow management
- **Cloud Integration**: AWS S3 for storage and EC2 for deployment
- **Monitoring**: Comprehensive logging and data quality checks

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Transform     │    │   Data Store    │
│                 │    │                 │    │                 │
│ • CSV Files     │───▶│ • Pandas        │───▶│ • PostgreSQL    │
│ • REST APIs     │    │ • Data Cleaning │    │ • Analytics     │
│ • Streaming     │    │ • Validation    │    │ • Raw Data      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Orchestration │    │   Cloud Storage │    │   Monitoring    │
│                 │    │                 │    │                 │
│ • Airflow DAGs  │    │ • AWS S3        │    │ • Logs          │
│ • Scheduling    │    │ • Backups       │    │ • Quality Checks│
│ • Dependencies  │    │ • Archiving     │    │ • Alerts        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🛠️ Tech Stack

### Core Technologies
- **Python 3.8+**: Primary programming language
- **Pandas**: Data manipulation and analysis
- **SQLAlchemy**: Database ORM and connection management
- **PostgreSQL**: Primary data warehouse
- **Apache Airflow**: Workflow orchestration

### Cloud & Infrastructure
- **AWS S3**: Object storage for data lakes and backups
- **AWS EC2**: Compute infrastructure for deployment
- **Docker**: Containerization for consistent environments
- **Docker Compose**: Multi-container orchestration

### Development & Testing
- **pytest**: Unit testing framework
- **black**: Code formatting
- **flake8**: Code linting
- **python-dotenv**: Environment configuration

## 📁 Project Structure

```
data-engineering-pipeline/
├── src/                          # Source code modules
│   ├── extract/                  # Data extraction components
│   │   ├── csv_extractor.py      # CSV file extraction
│   │   └── api_extractor.py      # API data extraction
│   ├── transform/                # Data transformation components
│   │   └── data_transformer.py   # Data cleaning and feature engineering
│   ├── load/                     # Data loading components
│   │   └── database_loader.py    # Database loading utilities
│   ├── orchestration/            # Pipeline orchestration
│   │   └── etl_pipeline.py       # Main ETL pipeline orchestrator
│   └── aws/                      # AWS integration
│       └── s3_integration.py      # S3 storage operations
├── dags/                         # Airflow DAGs
│   └── data_pipeline_dag.py      # Main pipeline DAG
├── config/                       # Configuration files
│   └── config.py                 # Application configuration
├── scripts/                      # Utility scripts
│   ├── init_database.sql         # Database initialization
│   └── deploy_ec2.sh             # EC2 deployment script
├── data/                         # Data directories
│   ├── raw/                      # Raw input data
│   └── processed/                # Processed data
├── Cloud_Dashboard/              # Interactive dashboard
│   └── index.html                # Live demo dashboard
├── tests/                        # Test files
├── logs/                         # Log files
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variables template
└── README.md                     # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- Docker and Docker Compose (for local development)
- AWS CLI and credentials (for cloud deployment)

### Local Setup

1. **Clone and Setup**
   ```bash
   git clone https://github.com/manasi-lang/data-engineering-pipeline.git
   cd "data engg project"
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Setup Database**
   ```bash
   # Start PostgreSQL (using Docker)
   docker run --name postgres-pipeline -e POSTGRES_PASSWORD=password -e POSTGRES_DB=data_pipeline -p 5432:5432 -d postgres:13
   
   # Initialize database schema
   psql -h localhost -U postgres -d data_pipeline -f scripts/init_database.sql
   ```

4. **Run Pipeline**
   ```bash
   python src/orchestration/etl_pipeline.py
   ```

### Docker Setup

1. **Using Docker Compose**
   ```bash
   docker-compose up -d
   ```

2. **Access Services**
   - Airflow Web UI: http://localhost:8080
   - PostgreSQL: localhost:5432

### AWS Deployment

1. **Deploy to EC2**
   ```bash
   # Make sure AWS CLI is configured
   aws configure
   
   # Run deployment script
   ./scripts/deploy_ec2.sh
   ```

2. **Access Deployed Services**
   - Airflow Web UI: http://<EC2-PUBLIC-IP>:8080
   - PostgreSQL: <EC2-PUBLIC-IP>:5432

## 📊 Usage Examples

### CSV Data Pipeline

```python
from src.orchestration.etl_pipeline import ETLPipeline

# Initialize pipeline
pipeline = ETLPipeline("csv_pipeline")

# Configure CSV job
csv_config = {
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
    }
}

# Run pipeline
results = pipeline.run_full_pipeline(csv_config)
print(f"Pipeline results: {results}")
```

### API Data Pipeline

```python
# Configure API job
api_config = {
    'products_job': {
        'type': 'api',
        'table_name': 'raw_data.products',
        'endpoint': 'products',
        'params': {'limit': 100},
        'transformations': {
            'type_mapping': {
                'created_date': 'datetime'
            }
        },
        'load_strategy': 'append'
    }
}

# Run pipeline
results = pipeline.run_full_pipeline(api_config)
```

### AWS S3 Integration

```python
from src.aws.s3_integration import S3Integration

# Initialize S3
s3 = S3Integration(
    aws_access_key_id='your_key',
    aws_secret_access_key='your_secret',
    bucket_name='data-pipeline-bucket'
)

# Upload data to S3
s3.upload_dataframe_to_s3(dataframe, 'raw/users.csv')

# Download data from S3
downloaded_df = s3.read_s3_to_dataframe('raw/users.csv')
```

## 🔧 Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=data_pipeline
DB_USER=postgres
DB_PASSWORD=password

# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=data-pipeline-bucket

# API Configuration
API_BASE_URL=https://api.example.com
API_KEY=your_api_key
```

### Database Schema

The pipeline uses a three-schema approach:

- **raw_data**: Raw extracted data
- **processed_data**: Transformed and aggregated data
- **analytics**: Business intelligence and metrics

## 📈 Features

### Data Extraction
- **CSV Files**: Multiple file support, automatic type inference
- **REST APIs**: Pagination support, error handling, retry logic
- **Error Recovery**: Comprehensive logging and retry mechanisms

### Data Transformation
- **Data Cleaning**: Duplicate removal, missing value handling
- **Type Conversion**: Automatic and manual type mapping
- **Feature Engineering**: Derived features, date extraction, categorical encoding
- **Data Validation**: Custom validation rules and quality checks

### Data Loading
- **Batch Processing**: Configurable chunk sizes for large datasets
- **Upsert Operations**: Conflict resolution and data updates
- **Performance**: Optimized loading with multi-insert operations

### Orchestration
- **Airflow Integration**: Professional workflow management
- **Scheduling**: Cron-based scheduling and dependency management
- **Monitoring**: Real-time pipeline status and alerting
- **Error Handling**: Automatic retries and failure notifications

### Cloud Integration
- **S3 Storage**: Data lake implementation with automatic archiving
- **EC2 Deployment**: One-click deployment to AWS
- **Scalability**: Horizontal scaling capabilities

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test file
pytest tests/test_etl_pipeline.py
```

## 📊 Monitoring & Logging

### Pipeline Monitoring
- **Airflow UI**: Real-time pipeline status
- **Database Logs**: ETL job execution logs
- **System Logs**: Application and error logs

### Data Quality
- **Validation Rules**: Custom data quality checks
- **Metrics Tracking**: Automated quality metrics
- **Alerting**: Quality threshold violations

## 🔒 Security

### Best Practices
- **Environment Variables**: Sensitive data in environment files
- **IAM Roles**: Principle of least privilege for AWS
- **Database Security**: Encrypted connections and access controls
- **API Security**: Key-based authentication

## 🚀 Production Deployment

### AWS Architecture
```
Internet Gateway
    │
    ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   EC2       │    │   RDS       │    │   S3        │
│             │    │             │    │             │
│ • Airflow   │    │ • PostgreSQL│    │ • Data Lake │
│ • Pipeline  │    │ • Analytics │    │ • Backups   │
│ • Docker    │    │ • Raw Data  │    │ • Archive   │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Deployment Steps
1. **Infrastructure Setup**: VPC, security groups, IAM roles
2. **Application Deployment**: Docker containers on EC2
3. **Database Setup**: RDS PostgreSQL with automated backups
4. **Monitoring Setup**: CloudWatch, logging, and alerting
5. **Data Pipeline**: Scheduled ETL jobs with Airflow

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

