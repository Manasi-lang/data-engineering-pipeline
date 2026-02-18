#!/usr/bin/env python3
"""
Local Pipeline Runner - No Docker Required
Run the complete data engineering pipeline locally
"""

import sys
import os
import subprocess
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ is required")
        return False
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    return True

def install_dependencies():
    """Install required Python packages"""
    print("📦 Installing Python dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def setup_postgres():
    """Setup PostgreSQL locally or provide instructions"""
    print("🗄️  PostgreSQL Setup Required")
    print("Option 1: Install PostgreSQL locally")
    print("  - macOS: brew install postgresql")
    print("  - Ubuntu: sudo apt-get install postgresql postgresql-contrib")
    print("  - Windows: Download from https://www.postgresql.org/download/")
    print("")
    print("Option 2: Use cloud PostgreSQL (AWS RDS, ElephantSQL, etc.)")
    print("")
    print("After setup, update .env file with your database credentials")
    return True

def run_pipeline():
    """Run the ETL pipeline"""
    print("🚀 Running Data Engineering Pipeline...")
    
    try:
        # Import and run pipeline
        from src.orchestration.etl_pipeline import ETLPipeline
        
        # Sample pipeline configuration
        pipeline_config = {
            'users_etl': {
                'type': 'csv',
                'table_name': 'raw_data.users',
                'file_paths': ['data/raw/sample_users.csv'],
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
                'file_paths': ['data/raw/sample_products.csv'],
                'transformations': {
                    'type_mapping': {
                        'created_date': 'datetime',
                        'updated_date': 'datetime'
                    }
                },
                'load_strategy': 'replace'
            },
            'sales_etl': {
                'type': 'csv',
                'table_name': 'raw_data.sales',
                'file_paths': ['data/raw/sample_sales.csv'],
                'transformations': {
                    'type_mapping': {
                        'sale_date': 'datetime'
                    },
                    'filters': {
                        'total_amount': {'min': 0}
                    }
                },
                'load_strategy': 'replace'
            }
        }
        
        # Initialize and run pipeline
        pipeline = ETLPipeline("local_pipeline")
        results = pipeline.run_full_pipeline(pipeline_config)
        
        print("\n📊 Pipeline Results:")
        for job, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"  {job}: {status}")
        
        print(f"\n📋 Pipeline Summary:")
        summary = pipeline.get_pipeline_summary()
        print(f"  Total Steps: {summary['total_steps']}")
        print(f"  Successful: {summary['summary']['successful_steps']}")
        print(f"  Failed: {summary['summary']['failed_steps']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        return False

def main():
    """Main execution function"""
    print("🚀 Data Engineering Pipeline - Local Setup")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        return False
    
    # Install dependencies
    if not install_dependencies():
        return False
    
    # Setup PostgreSQL instructions
    setup_postgres()
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("⚠️  .env file not found. Copying from .env.example...")
        import shutil
        shutil.copy('.env.example', '.env')
        print("📝 Please update .env file with your database credentials")
        print("   Then run this script again")
        return False
    
    # Run pipeline
    return run_pipeline()

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Pipeline completed successfully!")
    else:
        print("\n💥 Pipeline failed. Please check the error messages above.")
        sys.exit(1)
