#!/bin/bash

# EC2 Deployment Script for Data Engineering Pipeline
# This script sets up the complete data engineering pipeline on an EC2 instance

set -e

# Configuration
INSTANCE_TYPE="t3.medium"
AMI_ID="ami-0c02fb55956c7d316"  # Amazon Linux 2
KEY_NAME="data-pipeline-key"
SECURITY_GROUP_NAME="data-pipeline-sg"
REGION="us-east-1"
TAG_NAME="Data-Pipeline-Server"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if AWS CLI is installed
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    log_info "AWS CLI found"
}

# Check if AWS credentials are configured
check_aws_credentials() {
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Please run 'aws configure' first."
        exit 1
    fi
    log_info "AWS credentials configured"
}

# Create security group
create_security_group() {
    log_info "Creating security group..."
    
    SG_ID=$(aws ec2 create-security-group \
        --group-name "$SECURITY_GROUP_NAME" \
        --description "Security group for Data Engineering Pipeline" \
        --region "$REGION" \
        --query 'GroupId' \
        --output text)
    
    # Add rules
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 22 \
        --cidr 0.0.0.0/0 \
        --region "$REGION"
    
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 8080 \
        --cidr 0.0.0.0/0 \
        --region "$REGION"
    
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 5432 \
        --cidr 0.0.0.0/0 \
        --region "$REGION"
    
    log_info "Security group created with ID: $SG_ID"
}

# Create key pair
create_key_pair() {
    log_info "Creating key pair..."
    
    if [ ! -f "$KEY_NAME.pem" ]; then
        aws ec2 create-key-pair \
            --key-name "$KEY_NAME" \
            --region "$REGION" \
            --query 'KeyMaterial' \
            --output text > "$KEY_NAME.pem"
        
        chmod 400 "$KEY_NAME.pem"
        log_info "Key pair created: $KEY_NAME.pem"
    else
        log_info "Key pair already exists: $KEY_NAME.pem"
    fi
}

# Launch EC2 instance
launch_ec2_instance() {
    log_info "Launching EC2 instance..."
    
    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG_NAME}]" \
        --region "$REGION" \
        --query 'Instances[0].InstanceId' \
        --output text)
    
    log_info "Instance launched with ID: $INSTANCE_ID"
    
    # Wait for instance to be running
    log_info "Waiting for instance to be running..."
    aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION"
    
    # Get public IP
    PUBLIC_IP=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --region "$REGION" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' \
        --output text)
    
    log_info "Instance is running at IP: $PUBLIC_IP"
}

# Setup instance
setup_instance() {
    log_info "Setting up instance..."
    
    # Wait a bit more for SSH to be ready
    sleep 30
    
    # Create setup script
    cat > setup_instance.sh << 'EOF'
#!/bin/bash
set -e

# Update system
sudo yum update -y

# Install Docker
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -a -G docker ec2-user

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install Python and development tools
sudo yum install -y python3 python3-pip git postgresql postgresql-server

# Install Python dependencies
pip3 install --user pandas numpy sqlalchemy psycopg2-binary requests boto3 apache-airflow python-dotenv pytest black flake8

# Create project directory
mkdir -p /home/ec2-user/data-pipeline
cd /home/ec2-user/data-pipeline

# Create Docker Compose file for PostgreSQL and Airflow
cat > docker-compose.yml << 'DOCKER_COMPOSE_EOF'
version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: data_pipeline
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init_database.sql:/docker-entrypoint-initdb.d/init_database.sql
    networks:
      - pipeline_network

  redis:
    image: redis:6-alpine
    ports:
      - "6379:6379"
    networks:
      - pipeline_network

  airflow-webserver:
    image: apache/airflow:2.8.0
    depends_on:
      - postgres
      - redis
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://postgres:password@postgres:5432/airflow
      - AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
    ports:
      - "8080:8080"
    command: webserver
    networks:
      - pipeline_network

  airflow-scheduler:
    image: apache/airflow:2.8.0
    depends_on:
      - postgres
      - redis
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://postgres:password@postgres:5432/airflow
      - AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
    command: scheduler
    networks:
      - pipeline_network

volumes:
  postgres_data:

networks:
  pipeline_network:
    driver: bridge
DOCKER_COMPOSE_EOF

# Create environment file
cat > .env << 'ENV_EOF'
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
API_BASE_URL=https://jsonplaceholder.typicode.com
API_KEY=your_api_key

# Airflow Configuration
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://postgres:password@localhost:5432/airflow
ENV_EOF

# Create startup script
cat > start_pipeline.sh << 'START_EOF'
#!/bin/bash
set -e

echo "Starting Data Engineering Pipeline..."

# Start Docker services
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Initialize Airflow database
docker-compose exec airflow-webserver airflow db init

# Create Airflow user
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

echo "Pipeline started successfully!"
echo "Airflow Web UI: http://localhost:8080"
echo "PostgreSQL: localhost:5432"
echo "Default Airflow credentials: admin / admin"
START_EOF

chmod +x start_pipeline.sh

echo "Instance setup completed!"
EOF
    
    # Copy and execute setup script
    scp -o StrictHostKeyChecking=no -i "$KEY_NAME.pem" setup_instance.sh ec2-user@"$PUBLIC_IP":~/
    ssh -o StrictHostKeyChecking=no -i "$KEY_NAME.pem" ec2-user@"$PUBLIC_IP" "chmod +x setup_instance.sh && ./setup_instance.sh"
    
    log_info "Instance setup completed"
}

# Deploy application code
deploy_application() {
    log_info "Deploying application code..."
    
    # Create deployment script
    cat > deploy_app.sh << 'EOF'
#!/bin/bash
set -e

cd /home/ec2-user/data-pipeline

# Create project structure
mkdir -p {src/{extract,transform,load,orchestration,aws},data/{raw,processed},config,logs,tests,dags,scripts,docs}

# Copy application files (these would be uploaded separately)
echo "Application structure created"
echo "Please upload your source code files to the respective directories"
EOF
    
    # Copy and execute deployment script
    scp -o StrictHostKeyChecking=no -i "$KEY_NAME.pem" deploy_app.sh ec2-user@"$PUBLIC_IP":~/
    ssh -o StrictHostKeyChecking=no -i "$KEY_NAME.pem" ec2-user@"$PUBLIC_IP" "chmod +x deploy_app.sh && ./deploy_app.sh"
    
    log_info "Application deployment structure created"
}

# Upload source code
upload_source_code() {
    log_info "Uploading source code..."
    
    # Create tarball of source code
    tar -czf source_code.tar.gz src/ config/ dags/ scripts/ requirements.txt .env.example
    
    # Upload to instance
    scp -o StrictHostKeyChecking=no -i "$KEY_NAME.pem" source_code.tar.gz ec2-user@"$PUBLIC_IP":~/data-pipeline/
    
    # Extract on instance
    ssh -o StrictHostKeyChecking=no -i "$KEY_NAME.pem" ec2-user@"$PUBLIC_IP" \
        "cd /home/ec2-user/data-pipeline && tar -xzf source_code.tar.gz"
    
    # Clean up
    rm source_code.tar.gz
    
    log_info "Source code uploaded successfully"
}

# Start services
start_services() {
    log_info "Starting services..."
    
    ssh -o StrictHostKeyChecking=no -i "$KEY_NAME.pem" ec2-user@"$PUBLIC_IP" \
        "cd /home/ec2-user/data-pipeline && ./start_pipeline.sh"
    
    log_info "Services started successfully"
}

# Display connection information
display_info() {
    echo ""
    echo "=========================================="
    echo "🚀 DEPLOYMENT COMPLETED SUCCESSFULLY! 🚀"
    echo "=========================================="
    echo ""
    echo "Instance Information:"
    echo "  Public IP: $PUBLIC_IP"
    echo "  Instance ID: $INSTANCE_ID"
    echo "  Key Pair: $KEY_NAME.pem"
    echo ""
    echo "Access Information:"
    echo "  SSH: ssh -i $KEY_NAME.pem ec2-user@$PUBLIC_IP"
    echo "  Airflow Web UI: http://$PUBLIC_IP:8080"
    echo "  PostgreSQL: $PUBLIC_IP:5432"
    echo ""
    echo "Default Credentials:"
    echo "  Airflow: admin / admin"
    echo "  PostgreSQL: postgres / password"
    echo ""
    echo "Next Steps:"
    echo "  1. Access Airflow Web UI at http://$PUBLIC_IP:8080"
    echo "  2. Update AWS credentials in .env file"
    echo "  3. Upload your data files to data/raw/ directory"
    echo "  4. Trigger the data pipeline from Airflow"
    echo ""
    echo "To stop the instance:"
    echo "  aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region $REGION"
    echo ""
}

# Main execution
main() {
    log_info "Starting EC2 deployment for Data Engineering Pipeline..."
    
    check_aws_cli
    check_aws_credentials
    create_security_group
    create_key_pair
    launch_ec2_instance
    setup_instance
    upload_source_code
    start_services
    display_info
    
    log_info "Deployment completed successfully!"
}

# Run main function
main "$@"
