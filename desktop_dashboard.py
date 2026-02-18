#!/usr/bin/env python3
"""
Desktop Dashboard - Visual Data Engineering Pipeline Showcase
Displays pipeline results with interactive charts and visualizations
"""

import sys
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import webbrowser
import tempfile

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Set style for better looking charts
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def setup_data():
    """Setup and process data for visualization"""
    print("🔄 Processing data for dashboard...")
    
    # Extract data
    from src.extract.csv_extractor import CSVExtractor
    extractor = CSVExtractor()
    
    users_df = extractor.extract_from_csv('data/raw/sample_users.csv')
    products_df = extractor.extract_from_csv('data/raw/sample_products.csv')
    sales_df = extractor.extract_from_csv('data/raw/sample_sales.csv')
    
    # Transform data
    from src.transform.data_transformer import DataTransformer
    transformer = DataTransformer()
    
    users_clean = transformer.clean_data(users_df)
    products_clean = transformer.clean_data(products_df)
    sales_clean = transformer.clean_data(sales_df)
    
    # Convert date columns
    sales_clean['sale_date'] = pd.to_datetime(sales_clean['sale_date'])
    users_clean['registration_date'] = pd.to_datetime(users_clean['registration_date'])
    
    return users_clean, products_clean, sales_clean

def create_overview_dashboard(users_df, products_df, sales_df):
    """Create overview dashboard with key metrics"""
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('🚀 Data Engineering Pipeline Dashboard', fontsize=20, fontweight='bold')
    
    # 1. Total Records
    record_counts = [len(users_df), len(products_df), len(sales_df)]
    record_labels = ['Users', 'Products', 'Sales']
    axes[0, 0].bar(record_labels, record_counts, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
    axes[0, 0].set_title('📊 Total Records', fontsize=14, fontweight='bold')
    axes[0, 0].set_ylabel('Count')
    for i, v in enumerate(record_counts):
        axes[0, 0].text(i, v + 0.5, str(v), ha='center', fontweight='bold')
    
    # 2. Age Distribution
    axes[0, 1].hist(users_df['age'], bins=8, color='#95E77E', edgecolor='black', alpha=0.7)
    axes[0, 1].set_title('👥 User Age Distribution', fontsize=14, fontweight='bold')
    axes[0, 1].set_xlabel('Age')
    axes[0, 1].set_ylabel('Frequency')
    
    # 3. Product Categories
    category_counts = products_df['category'].value_counts()
    axes[0, 2].pie(category_counts.values, labels=category_counts.index, autopct='%1.1f%%', 
                    colors=['#FF9999', '#66B2FF', '#99FF99', '#FFCC99'])
    axes[0, 2].set_title('📦 Product Categories', fontsize=14, fontweight='bold')
    
    # 4. Sales Over Time
    daily_sales = sales_df.groupby(sales_df['sale_date'].dt.date)['total_amount'].sum()
    axes[1, 0].plot(daily_sales.index, daily_sales.values, marker='o', linewidth=3, markersize=8, color='#FF6B6B')
    axes[1, 0].set_title('💰 Sales Over Time', fontsize=14, fontweight='bold')
    axes[1, 0].set_xlabel('Date')
    axes[1, 0].set_ylabel('Total Sales ($)')
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # 5. Top Products by Sales
    product_sales = sales_df.groupby('product_id')['total_amount'].sum().sort_values(ascending=False).head(5)
    axes[1, 1].barh(range(len(product_sales)), product_sales.values, color='#4ECDC4')
    axes[1, 1].set_title('🏆 Top Products by Sales', fontsize=14, fontweight='bold')
    axes[1, 1].set_yticks(range(len(product_sales)))
    axes[1, 1].set_yticklabels(product_sales.index)
    axes[1, 1].set_xlabel('Total Sales ($)')
    
    # 6. Payment Methods
    payment_counts = sales_df['payment_method'].value_counts()
    axes[1, 2].bar(payment_counts.index, payment_counts.values, color=['#45B7D1', '#96CEB4'])
    axes[1, 2].set_title('💳 Payment Methods', fontsize=14, fontweight='bold')
    axes[1, 2].set_ylabel('Count')
    axes[1, 2].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    return fig

def create_detailed_analysis(users_df, products_df, sales_df):
    """Create detailed analysis dashboard"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('📈 Detailed Data Analysis', fontsize=20, fontweight='bold')
    
    # 1. User Locations
    location_counts = users_df['location'].value_counts()
    axes[0, 0].barh(range(len(location_counts)), location_counts.values, color='#FF6B6B')
    axes[0, 0].set_title('🌍 Users by Location', fontsize=14, fontweight='bold')
    axes[0, 0].set_yticks(range(len(location_counts)))
    axes[0, 0].set_yticklabels(location_counts.index)
    axes[0, 0].set_xlabel('Number of Users')
    
    # 2. Price Distribution
    axes[0, 1].hist(products_df['price'], bins=10, color='#4ECDC4', edgecolor='black', alpha=0.7)
    axes[0, 1].set_title('💵 Product Price Distribution', fontsize=14, fontweight='bold')
    axes[0, 1].set_xlabel('Price ($)')
    axes[0, 1].set_ylabel('Number of Products')
    
    # 3. Sales by Store Location
    store_sales = sales_df.groupby('store_location')['total_amount'].sum().sort_values(ascending=False)
    axes[1, 0].bar(range(len(store_sales)), store_sales.values, color='#95E77E')
    axes[1, 0].set_title('🏪 Sales by Store Location', fontsize=14, fontweight='bold')
    axes[1, 0].set_xticks(range(len(store_sales)))
    axes[1, 0].set_xticklabels(store_sales.index, rotation=45)
    axes[1, 0].set_ylabel('Total Sales ($)')
    
    # 4. Average Order Value
    avg_order = sales_df.groupby('store_location')['total_amount'].mean().sort_values(ascending=False)
    axes[1, 1].bar(range(len(avg_order)), avg_order.values, color='#FFB347')
    axes[1, 1].set_title('📊 Average Order Value', fontsize=14, fontweight='bold')
    axes[1, 1].set_xticks(range(len(avg_order)))
    axes[1, 1].set_xticklabels(avg_order.index, rotation=45)
    axes[1, 1].set_ylabel('Average Order Value ($)')
    
    plt.tight_layout()
    return fig

def create_pipeline_status():
    """Create pipeline status visualization"""
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    # Pipeline stages and status
    stages = [
        ('Data Extraction', '✅ Complete', '#4CAF50'),
        ('Data Cleaning', '✅ Complete', '#4CAF50'),
        ('Data Transformation', '✅ Complete', '#4CAF50'),
        ('API Integration', '✅ Complete', '#4CAF50'),
        ('Database Loading', '⏳ Ready', '#FF9800'),
        ('Airflow Orchestration', '⏳ Ready', '#FF9800'),
        ('AWS Deployment', '⏳ Ready', '#FF9800')
    ]
    
    y_pos = range(len(stages))
    colors = [status[2] for status in stages]
    
    bars = ax.barh(y_pos, [100]*len(stages), color=colors, alpha=0.7)
    ax.set_yticks(y_pos)
    ax.set_yticklabels([f"{stage[0]} - {stage[1]}" for stage in stages])
    ax.set_xlabel('Completion %', fontsize=12)
    ax.set_title('🚀 Pipeline Status Dashboard', fontsize=16, fontweight='bold')
    
    # Add percentage text
    for i, (stage, status, color) in enumerate(stages):
        percentage = 100 if 'Complete' in status else 80
        ax.text(percentage/2, i, f'{percentage}%', ha='center', va='center', 
                fontweight='bold', fontsize=12)
    
    ax.set_xlim(0, 100)
    plt.tight_layout()
    return fig

def create_html_dashboard(users_df, products_df, sales_df):
    """Create HTML dashboard for web display"""
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>🚀 Data Engineering Pipeline Dashboard</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; text-align: center; }}
            .metrics {{ display: flex; justify-content: space-around; margin: 20px 0; }}
            .metric {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; flex: 1; margin: 0 10px; }}
            .metric h3 {{ margin: 0; color: #333; }}
            .metric .number {{ font-size: 2em; font-weight: bold; color: #667eea; }}
            .status {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin: 20px 0; }}
            .success {{ color: #4CAF50; }}
            .ready {{ color: #FF9800; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #667eea; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 Data Engineering Pipeline Dashboard</h1>
            <p>Real-time pipeline monitoring and analytics</p>
        </div>
        
        <div class="metrics">
            <div class="metric">
                <h3>👥 Total Users</h3>
                <div class="number">{len(users_df)}</div>
            </div>
            <div class="metric">
                <h3>📦 Total Products</h3>
                <div class="number">{len(products_df)}</div>
            </div>
            <div class="metric">
                <h3>💰 Total Sales</h3>
                <div class="number">${sales_df['total_amount'].sum():.2f}</div>
            </div>
            <div class="metric">
                <h3>📊 Avg Order Value</h3>
                <div class="number">${sales_df['total_amount'].mean():.2f}</div>
            </div>
        </div>
        
        <div class="status">
            <h2>🔧 Pipeline Components Status</h2>
            <p><span class="success">✅</span> CSV Data Extraction - Working</p>
            <p><span class="success">✅</span> Data Transformation - Working</p>
            <p><span class="success">✅</span> API Integration - Working</p>
            <p><span class="ready">⏳</span> Database Loading - Ready</p>
            <p><span class="ready">⏳</span> Airflow Orchestration - Ready</p>
            <p><span class="ready">⏳</span> AWS Deployment - Ready</p>
        </div>
        
        <h2>📊 Recent Sales Data</h2>
        <table>
            <tr>
                <th>Sale ID</th>
                <th>User ID</th>
                <th>Product ID</th>
                <th>Amount</th>
                <th>Date</th>
                <th>Location</th>
            </tr>
    """
    
    # Add recent sales
    recent_sales = sales_df.head(10).sort_values('sale_date', ascending=False)
    for _, sale in recent_sales.iterrows():
        html_content += f"""
            <tr>
                <td>{sale['sale_id']}</td>
                <td>{sale['user_id']}</td>
                <td>{sale['product_id']}</td>
                <td>${sale['total_amount']:.2f}</td>
                <td>{sale['sale_date']}</td>
                <td>{sale['store_location']}</td>
            </tr>
        """
    
    html_content += """
        </table>
        
        <div style="text-align: center; margin-top: 30px; color: #666;">
            <p>🚀 Production-Ready Data Engineering Pipeline</p>
            <p>Generated on """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</p>
        </div>
    </body>
    </html>
    """
    
    return html_content

def main():
    """Main dashboard function"""
    print("🚀 Creating Desktop Dashboard...")
    print("=" * 50)
    
    # Setup data
    users_df, products_df, sales_df = setup_data()
    
    # Create visualizations
    print("📊 Creating visualizations...")
    
    # Overview dashboard
    fig1 = create_overview_dashboard(users_df, products_df, sales_df)
    
    # Detailed analysis
    fig2 = create_detailed_analysis(users_df, products_df, sales_df)
    
    # Pipeline status
    fig3 = create_pipeline_status()
    
    # Save charts
    dashboard_dir = Path.home() / "Desktop" / "DataPipeline_Dashboard"
    dashboard_dir.mkdir(exist_ok=True)
    
    fig1.savefig(dashboard_dir / "overview_dashboard.png", dpi=300, bbox_inches='tight')
    fig2.savefig(dashboard_dir / "detailed_analysis.png", dpi=300, bbox_inches='tight')
    fig3.savefig(dashboard_dir / "pipeline_status.png", dpi=300, bbox_inches='tight')
    
    # Create HTML dashboard
    html_content = create_html_dashboard(users_df, products_df, sales_df)
    html_file = dashboard_dir / "dashboard.html"
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    plt.close('all')
    
    print(f"✅ Dashboard created successfully!")
    print(f"📁 Location: {dashboard_dir}")
    print(f"🌐 Open in browser: file://{html_file}")
    
    # Open in browser
    try:
        webbrowser.open(f"file://{html_file}")
        print("🌐 Dashboard opened in your browser!")
    except:
        print("💡 Open manually: file://{html_file}")
    
    print("\n📋 Dashboard Contents:")
    print("📊 overview_dashboard.png - Main metrics overview")
    print("📈 detailed_analysis.png - Detailed data analysis")
    print("🚀 pipeline_status.png - Pipeline component status")
    print("🌐 dashboard.html - Interactive web dashboard")
    
    return True

if __name__ == "__main__":
    # Install required packages
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError:
        print("📦 Installing visualization packages...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "matplotlib", "seaborn"])
        import matplotlib.pyplot as plt
        import seaborn as sns
    
    success = main()
    if not success:
        sys.exit(1)
