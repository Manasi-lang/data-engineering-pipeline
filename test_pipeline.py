#!/usr/bin/env python3
"""
Quick Pipeline Test - No Database Required
Test the extraction and transformation components
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_csv_extraction():
    """Test CSV extraction"""
    print("📥 Testing CSV Extraction...")
    
    try:
        from src.extract.csv_extractor import CSVExtractor
        
        extractor = CSVExtractor()
        
        # Test users extraction
        users_df = extractor.extract_from_csv('data/raw/sample_users.csv')
        print(f"✅ Users: {len(users_df)} rows, {len(users_df.columns)} columns")
        
        # Test products extraction
        products_df = extractor.extract_from_csv('data/raw/sample_products.csv')
        print(f"✅ Products: {len(products_df)} rows, {len(products_df.columns)} columns")
        
        # Test sales extraction
        sales_df = extractor.extract_from_csv('data/raw/sample_sales.csv')
        print(f"✅ Sales: {len(sales_df)} rows, {len(sales_df.columns)} columns")
        
        return users_df, products_df, sales_df
        
    except Exception as e:
        print(f"❌ CSV Extraction failed: {e}")
        return None, None, None

def test_data_transformation(users_df, products_df, sales_df):
    """Test data transformation"""
    print("\n🔄 Testing Data Transformation...")
    
    try:
        from src.transform.data_transformer import DataTransformer
        
        transformer = DataTransformer()
        
        # Transform users data
        users_clean = transformer.clean_data(users_df)
        users_std = transformer.standardize_columns(users_clean)
        users_final = transformer.convert_data_types(users_std, {
            'registration_date': 'datetime',
            'last_active': 'datetime'
        })
        print(f"✅ Users transformed: {len(users_final)} rows")
        
        # Transform products data
        products_clean = transformer.clean_data(products_df)
        products_std = transformer.standardize_columns(products_clean)
        products_final = transformer.convert_data_types(products_std, {
            'created_date': 'datetime',
            'updated_date': 'datetime'
        })
        print(f"✅ Products transformed: {len(products_final)} rows")
        
        # Transform sales data
        sales_clean = transformer.clean_data(sales_df)
        sales_std = transformer.standardize_columns(sales_clean)
        sales_final = transformer.convert_data_types(sales_std, {
            'sale_date': 'datetime'
        })
        print(f"✅ Sales transformed: {len(sales_final)} rows")
        
        return users_final, products_final, sales_final
        
    except Exception as e:
        print(f"❌ Data Transformation failed: {e}")
        return None, None, None

def test_api_extraction():
    """Test API extraction"""
    print("\n🌐 Testing API Extraction...")
    
    try:
        from src.extract.api_extractor import APIExtractor
        
        # Using a public API for testing
        extractor = APIExtractor("https://jsonplaceholder.typicode.com")
        
        # Test users API
        users_df = extractor.extract_to_dataframe("users")
        print(f"✅ API Users: {len(users_df)} rows, {len(users_df.columns)} columns")
        
        return users_df
        
    except Exception as e:
        print(f"❌ API Extraction failed: {e}")
        return None

def display_sample_data(users_df, products_df, sales_df):
    """Display sample data"""
    print("\n📊 Sample Data Preview:")
    
    print("\n👥 Users Data:")
    print(users_df.head(3).to_string())
    
    print("\n📦 Products Data:")
    print(products_df.head(3).to_string())
    
    print("\n💰 Sales Data:")
    print(sales_df.head(3).to_string())

def main():
    """Main test function"""
    print("🧪 Data Engineering Pipeline - Quick Test")
    print("=" * 50)
    
    # Test CSV extraction
    users_df, products_df, sales_df = test_csv_extraction()
    
    if users_df is None or products_df is None or sales_df is None:
        print("❌ Cannot proceed with transformation tests")
        return False
    
    # Test data transformation
    users_final, products_final, sales_final = test_data_transformation(
        users_df, products_df, sales_df
    )
    
    if users_final is None or products_final is None or sales_final is None:
        print("❌ Cannot proceed with display")
        return False
    
    # Display sample data
    display_sample_data(users_final, products_final, sales_final)
    
    # Test API extraction (optional)
    api_users = test_api_extraction()
    
    print("\n🎉 All tests completed successfully!")
    print("\n📝 Next Steps:")
    print("1. Install Docker for full pipeline: ./scripts/install_docker_mac.sh")
    print("2. Or setup PostgreSQL locally for database operations")
    print("3. Update .env file with your credentials")
    print("4. Run full pipeline: python run_pipeline_local.py")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
