#!/usr/bin/env python3
"""
Cloud Dashboard Deployment - Deploy interactive dashboard to cloud
Creates a web-ready dashboard and deploys to free hosting
"""

import sys
import os
from pathlib import Path
import pandas as pd
import json
import tempfile
import webbrowser
import requests
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def setup_data():
    """Setup and process data for cloud dashboard"""
    print("🔄 Processing data for cloud dashboard...")
    
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

def create_cloud_dashboard_data(users_df, products_df, sales_df):
    """Create data for cloud dashboard"""
    
    # Calculate metrics
    metrics = {
        'total_users': len(users_df),
        'total_products': len(products_df),
        'total_sales': float(sales_df['total_amount'].sum()),
        'avg_order_value': float(sales_df['total_amount'].mean()),
        'total_revenue': float(sales_df['total_amount'].sum()),
        'unique_locations': len(users_df['location'].unique()),
        'product_categories': len(products_df['category'].unique())
    }
    
    # Sales over time
    daily_sales = sales_df.groupby(sales_df['sale_date'].dt.date)['total_amount'].sum().reset_index()
    sales_timeline = [
        {'date': str(row['sale_date']), 'sales': float(row['total_amount'])}
        for _, row in daily_sales.iterrows()
    ]
    
    # Top products
    product_sales = sales_df.groupby('product_id')['total_amount'].sum().sort_values(ascending=False).head(5)
    top_products = [
        {'product': product, 'sales': float(sales)}
        for product, sales in product_sales.items()
    ]
    
    # User demographics
    age_groups = users_df.groupby(pd.cut(users_df['age'], bins=[0, 25, 35, 45, 100], labels=['18-24', '25-34', '35-44', '45+']))['age'].count()
    demographics = {
        'age_groups': age_groups.to_dict(),
        'locations': users_df['location'].value_counts().to_dict()
    }
    
    # Recent sales
    recent_sales = sales_df.sort_values('sale_date', ascending=False).head(10)
    sales_data = [
        {
            'sale_id': row['sale_id'],
            'user_id': row['user_id'],
            'product_id': row['product_id'],
            'amount': float(row['total_amount']),
            'date': str(row['sale_date']),
            'location': row['store_location'],
            'payment_method': row['payment_method']
        }
        for _, row in recent_sales.iterrows()
    ]
    
    return {
        'metrics': metrics,
        'sales_timeline': sales_timeline,
        'top_products': top_products,
        'demographics': demographics,
        'recent_sales': sales_data,
        'last_updated': datetime.now().isoformat()
    }

def create_static_dashboard_html(data):
    """Create a self-contained HTML dashboard"""
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚀 Data Engineering Pipeline Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            min-height: 100vh; 
            color: #333;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .header {{ 
            background: rgba(255,255,255,0.95); 
            padding: 30px; 
            border-radius: 15px; 
            text-align: center; 
            margin-bottom: 30px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        }}
        .header h1 {{ font-size: 2.5em; margin-bottom: 10px; background: linear-gradient(45deg, #667eea, #764ba2); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
        .header p {{ font-size: 1.2em; color: #666; }}
        .metrics {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
            gap: 20px; 
            margin-bottom: 30px; 
        }}
        .metric {{ 
            background: rgba(255,255,255,0.95); 
            padding: 25px; 
            border-radius: 15px; 
            text-align: center; 
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }}
        .metric:hover {{ transform: translateY(-5px); }}
        .metric h3 {{ font-size: 1em; margin-bottom: 10px; color: #666; }}
        .metric .number {{ font-size: 2.5em; font-weight: bold; color: #667eea; margin-bottom: 5px; }}
        .metric .label {{ font-size: 0.9em; color: #999; }}
        .charts {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); 
            gap: 20px; 
            margin-bottom: 30px; 
        }}
        .chart {{ 
            background: rgba(255,255,255,0.95); 
            padding: 25px; 
            border-radius: 15px; 
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        }}
        .chart h3 {{ margin-bottom: 20px; color: #333; }}
        .table {{ 
            background: rgba(255,255,255,0.95); 
            padding: 25px; 
            border-radius: 15px; 
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        }}
        .table h3 {{ margin-bottom: 20px; color: #333; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: #f8f9fa; font-weight: 600; color: #667eea; }}
        tr:hover {{ background: #f8f9fa; }}
        .status {{ 
            background: rgba(255,255,255,0.95); 
            padding: 20px; 
            border-radius: 15px; 
            margin-bottom: 20px; 
            text-align: center;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        }}
        .status-item {{ display: inline-block; margin: 10px; padding: 10px 20px; border-radius: 25px; font-weight: bold; }}
        .success {{ background: #4CAF50; color: white; }}
        .ready {{ background: #FF9800; color: white; }}
        .footer {{ 
            text-align: center; 
            padding: 20px; 
            color: white; 
            margin-top: 30px;
        }}
        @media (max-width: 768px) {{
            .metrics {{ grid-template-columns: 1fr; }}
            .charts {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Data Engineering Pipeline Dashboard</h1>
            <p>Real-time analytics and monitoring • Production-ready ETL pipeline</p>
        </div>
        
        <div class="metrics">
            <div class="metric">
                <h3>👥 Total Users</h3>
                <div class="number">{data['metrics']['total_users']}</div>
                <div class="label">Active users</div>
            </div>
            <div class="metric">
                <h3>📦 Total Products</h3>
                <div class="number">{data['metrics']['total_products']}</div>
                <div class="label">Products catalog</div>
            </div>
            <div class="metric">
                <h3>💰 Total Revenue</h3>
                <div class="number">${data['metrics']['total_sales']:,.2f}</div>
                <div class="label">Total sales</div>
            </div>
            <div class="metric">
                <h3>📊 Avg Order Value</h3>
                <div class="number">${data['metrics']['avg_order_value']:,.2f}</div>
                <div class="label">Per transaction</div>
            </div>
        </div>
        
        <div class="status">
            <h3>🔧 Pipeline Components</h3>
            <span class="status-item success">✅ Data Extraction</span>
            <span class="status-item success">✅ Data Transformation</span>
            <span class="status-item success">✅ API Integration</span>
            <span class="status-item ready">⏳ Database Loading</span>
            <span class="status-item ready">⏳ Cloud Deployment</span>
        </div>
        
        <div class="charts">
            <div class="chart">
                <h3>📈 Sales Over Time</h3>
                <canvas id="salesChart" width="400" height="200"></canvas>
            </div>
            <div class="chart">
                <h3>🏆 Top Products</h3>
                <canvas id="productsChart" width="400" height="200"></canvas>
            </div>
        </div>
        
        <div class="table">
            <h3>💰 Recent Transactions</h3>
            <table>
                <thead>
                    <tr>
                        <th>Sale ID</th>
                        <th>Product</th>
                        <th>Amount</th>
                        <th>Date</th>
                        <th>Location</th>
                    </tr>
                </thead>
                <tbody>
"""
    
    # Add recent sales to table
    for sale in data['recent_sales'][:8]:
        html_content += f"""
                    <tr>
                        <td>{sale['sale_id']}</td>
                        <td>{sale['product_id']}</td>
                        <td>${sale['amount']:.2f}</td>
                        <td>{sale['date']}</td>
                        <td>{sale['location']}</td>
                    </tr>
        """
    
    html_content += f"""
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>🚀 Production-Ready Data Engineering Pipeline</p>
            <p>Last updated: {data['last_updated']}</p>
            <p>Built with Python, Pandas, and deployed to cloud</p>
        </div>
    </div>
    
    <script>
        // Sales over time chart
        const salesCtx = document.getElementById('salesChart').getContext('2d');
        new Chart(salesCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps([item['date'] for item in data['sales_timeline']])},
                datasets: [{{
                    label: 'Sales ($)',
                    data: {json.dumps([item['sales'] for item in data['sales_timeline']])},
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    borderWidth: 3,
                    tension: 0.4,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return '$' + value.toLocaleString();
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Top products chart
        const productsCtx = document.getElementById('productsChart').getContext('2d');
        new Chart(productsCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps([item['product'] for item in data['top_products']])},
                datasets: [{{
                    label: 'Sales ($)',
                    data: {json.dumps([item['sales'] for item in data['top_products']])},
                    backgroundColor: [
                        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'
                    ],
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return '$' + value.toLocaleString();
                            }}
                        }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
    """
    
    return html_content

def deploy_to_github_pages(html_content):
    """Deploy dashboard to GitHub Pages"""
    print("🚀 Deploying to GitHub Pages...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create dashboard files
        dashboard_file = temp_path / "index.html"
        with open(dashboard_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # Create README
        readme_content = f"""
# 🚀 Data Engineering Pipeline Dashboard

This is a cloud-hosted interactive dashboard showcasing a complete data engineering pipeline.

## Features:
- Real-time data processing
- Interactive charts and visualizations
- Multi-source data integration
- Production-ready ETL pipeline

## Live Dashboard:
Access the interactive dashboard at the GitHub Pages URL.

## Technology Stack:
- Python 3.8+
- Pandas for data processing
- Chart.js for visualizations
- GitHub Pages for hosting

Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        """
        
        with open(temp_path / "README.md", 'w') as f:
            f.write(readme_content)
        
        print(f"📁 Dashboard files created in: {temp_path}")
        print("📝 To deploy to GitHub Pages:")
        print("1. Create a new GitHub repository")
        print("2. Upload the files from the temporary directory")
        print("3. Enable GitHub Pages in repository settings")
        print("4. Your dashboard will be live at: https://[username].github.io/[repository]/")
        
        return temp_path

def deploy_to_netlify(html_content):
    """Deploy dashboard to Netlify (simpler alternative)"""
    print("🌐 Preparing for Netlify deployment...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create dashboard files
        dashboard_file = temp_path / "index.html"
        with open(dashboard_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"📁 Dashboard files created in: {temp_path}")
        print("🌐 To deploy to Netlify:")
        print("1. Go to https://netlify.com")
        print("2. Drag and drop the folder to deploy")
        print("3. Get instant public URL")
        print("4. Free hosting with custom domain support")
        
        return temp_path

def create_public_url_dashboard():
    """Create and prepare dashboard for public deployment"""
    print("🚀 Creating Cloud Dashboard...")
    print("=" * 50)
    
    # Setup data
    users_df, products_df, sales_df = setup_data()
    
    # Create dashboard data
    dashboard_data = create_cloud_dashboard_data(users_df, products_df, sales_df)
    
    # Create HTML dashboard
    html_content = create_static_dashboard_html(dashboard_data)
    
    # Save locally first
    dashboard_dir = Path.home() / "Desktop" / "Cloud_Dashboard"
    dashboard_dir.mkdir(exist_ok=True)
    
    html_file = dashboard_dir / "index.html"
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Cloud dashboard created!")
    print(f"📁 Local copy: {html_file}")
    
    # Open in browser
    try:
        webbrowser.open(f"file://{html_file}")
        print("🌐 Dashboard opened in your browser!")
    except:
        print(f"💡 Open manually: file://{html_file}")
    
    # Deployment options
    print("\n🌐 DEPLOYMENT OPTIONS:")
    print("=" * 30)
    
    print("\n1️⃣ NETLIFY (Easiest - Instant):")
    print("   🌐 Go to: https://netlify.com")
    print("   📁 Drag & drop: Cloud_Dashboard folder")
    print("   ⚡ Instant URL: https://[random-name].netlify.app")
    print("   💰 Free forever!")
    
    print("\n2️⃣ GITHUB PAGES (Professional):")
    print("   📁 Create GitHub repository")
    print("   📤 Upload Cloud_Dashboard folder")
    print("   🔧 Enable GitHub Pages")
    print("   🌐 URL: https://[username].github.io/[repo]/")
    print("   💰 Free with GitHub account")
    
    print("\n3️⃣ VERCEL (Modern):")
    print("   🌐 Go to: https://vercel.com")
    print("   📁 Import from GitHub or upload")
    print("   ⚡ Automatic deployments")
    print("   🌐 URL: https://[project].vercel.app")
    print("   💰 Free tier available")
    
    print("\n📋 QUICK DEPLOY STEPS:")
    print("1. Choose a platform above")
    print("2. Upload the Cloud_Dashboard folder")
    print("3. Get your public URL")
    print("4. Share your dashboard!")
    
    return dashboard_dir

def main():
    """Main deployment function"""
    print("🚀 Cloud Dashboard Deployment")
    print("=" * 50)
    
    # Create dashboard
    dashboard_dir = create_public_url_dashboard()
    
    print(f"\n✅ Dashboard ready for cloud deployment!")
    print(f"📁 Location: {dashboard_dir}")
    print("🌐 Ready for instant public access!")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
