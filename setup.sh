#!/bin/bash
# Setup script for NYC Taxi Hadoop Analysis

echo "🚕 NYC Taxi Hadoop Analysis Setup Script"
echo "======================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create shared directory structure
echo "📁 Creating directory structure..."
mkdir -p shared/data
mkdir -p saved_plots
mkdir -p screenshots

# Install Python dependencies
echo "🐍 Installing Python dependencies..."
pip install -r requirements.txt

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Download NYC Taxi data and place in shared/data/"
echo "2. Run: docker-compose up -d"
echo "3. Wait for containers to start (~2-3 minutes)"
echo "4. Move data to HDFS (see README.md for details)"
echo "5. Generate plots: python generate_plots.py"
echo "6. Launch dashboard: streamlit run app.py"
echo ""
echo "📚 For detailed instructions, see README.md"
