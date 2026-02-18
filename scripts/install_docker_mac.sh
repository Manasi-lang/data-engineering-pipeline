#!/bin/bash

# Docker Installation Script for macOS

echo "🐳 Installing Docker Desktop for macOS..."

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "📦 Installing Homebrew first..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# Install Docker Desktop using Homebrew
echo "📦 Installing Docker Desktop..."
brew install --cask docker

echo "✅ Docker Desktop installation started!"
echo "📝 Please follow the GUI instructions to complete installation"
echo "🔄 After installation, start Docker Desktop from Applications folder"
echo "⏳ Wait for Docker to start (green icon in menu bar)"
echo ""
echo "Then run: docker-compose up -d"
