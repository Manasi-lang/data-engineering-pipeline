#!/bin/bash

# Simple runner script to handle directory name with spaces
cd "$(dirname "$0")"

echo "🚀 Data Engineering Pipeline Runner"
echo "=================================="

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "❌ PostgreSQL not found. Installing..."
    if command -v brew &> /dev/null; then
        brew install postgresql
    else
        echo "Please install PostgreSQL manually from https://www.postgresql.org/download/"
        exit 1
    fi
fi

# Copy .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "✅ .env file created. Please edit it with your database credentials."
fi

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt

# Run the test pipeline
echo "🧪 Running pipeline test..."
python3 test_pipeline.py

echo ""
echo "🎉 Pipeline test completed!"
echo ""
echo "📝 Next steps:"
echo "1. Edit .env file with your database credentials"
echo "2. Run full pipeline: python3 run_pipeline_local.py"
echo "3. Or install Docker for complete setup: ./scripts/install_docker_mac.sh"
