-- Database initialization script for Data Engineering Pipeline

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS data_pipeline;

-- Use the database
\c data_pipeline;

-- Create schema for raw data
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Create schema for processed data
CREATE SCHEMA IF NOT EXISTS processed_data;

-- Create schema for analytics
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create users table
CREATE TABLE IF NOT EXISTS raw_data.users (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INTEGER,
    gender VARCHAR(10),
    location VARCHAR(255),
    registration_date DATE,
    last_active DATE,
    source_file VARCHAR(255),
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table
CREATE TABLE IF NOT EXISTS raw_data.products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2),
    description TEXT,
    brand VARCHAR(100),
    created_date DATE,
    updated_date DATE,
    source_file VARCHAR(255),
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create sales table
CREATE TABLE IF NOT EXISTS raw_data.sales (
    id SERIAL PRIMARY KEY,
    sale_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    sale_date DATE NOT NULL,
    store_location VARCHAR(255),
    payment_method VARCHAR(50),
    source_file VARCHAR(255),
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES raw_data.users(user_id),
    FOREIGN KEY (product_id) REFERENCES raw_data.products(product_id)
);

-- Create processed user analytics table
CREATE TABLE IF NOT EXISTS processed_data.user_analytics (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    age_group VARCHAR(20),
    location VARCHAR(255),
    total_purchases INTEGER DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0.00,
    avg_purchase_value DECIMAL(10,2) DEFAULT 0.00,
    favorite_category VARCHAR(100),
    customer_lifetime_days INTEGER,
    last_purchase_date DATE,
    first_purchase_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create product analytics table
CREATE TABLE IF NOT EXISTS processed_data.product_analytics (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    brand VARCHAR(100),
    total_sales INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    avg_price DECIMAL(10,2),
    unique_customers INTEGER DEFAULT 0,
    top_locations TEXT[],
    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create sales analytics table
CREATE TABLE IF NOT EXISTS processed_data.sales_analytics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    total_sales INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    avg_order_value DECIMAL(10,2) DEFAULT 0.00,
    unique_customers INTEGER DEFAULT 0,
    unique_products INTEGER DEFAULT 0,
    top_category VARCHAR(100),
    top_location VARCHAR(255),
    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create ETL job logs table
CREATE TABLE IF NOT EXISTS analytics.etl_job_logs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create data quality metrics table
CREATE TABLE IF NOT EXISTS analytics.data_quality_metrics (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(10,4),
    threshold_value DECIMAL(10,4),
    status VARCHAR(20) NOT NULL,
    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_users_email ON raw_data.users(email);
CREATE INDEX IF NOT EXISTS idx_users_location ON raw_data.users(location);
CREATE INDEX IF NOT EXISTS idx_users_registration_date ON raw_data.users(registration_date);

CREATE INDEX IF NOT EXISTS idx_products_category ON raw_data.products(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON raw_data.products(brand);
CREATE INDEX IF NOT EXISTS idx_products_created_date ON raw_data.products(created_date);

CREATE INDEX IF NOT EXISTS idx_sales_user_id ON raw_data.sales(user_id);
CREATE INDEX IF NOT EXISTS idx_sales_product_id ON raw_data.sales(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_date ON raw_data.sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_store_location ON raw_data.sales(store_location);

CREATE INDEX IF NOT EXISTS idx_user_analytics_user_id ON processed_data.user_analytics(user_id);
CREATE INDEX IF NOT EXISTS idx_product_analytics_product_id ON processed_data.product_analytics(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_analytics_date ON processed_data.sales_analytics(date);

CREATE INDEX IF NOT EXISTS idx_etl_logs_job_name ON analytics.etl_job_logs(job_name);
CREATE INDEX IF NOT EXISTS idx_etl_logs_start_time ON analytics.etl_job_logs(start_time);
CREATE INDEX IF NOT EXISTS idx_etl_logs_status ON analytics.etl_job_logs(status);

CREATE INDEX IF NOT EXISTS idx_data_quality_table_name ON analytics.data_quality_metrics(table_name);
CREATE INDEX IF NOT EXISTS idx_data_quality_measured_at ON analytics.data_quality_metrics(measured_at);

-- Grant permissions (adjust as needed for your environment)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO data_pipeline_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA processed_data TO data_pipeline_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO data_pipeline_user;

-- Grant usage on schemas
-- GRANT USAGE ON SCHEMA raw_data TO data_pipeline_user;
-- GRANT USAGE ON SCHEMA processed_data TO data_pipeline_user;
-- GRANT USAGE ON SCHEMA analytics TO data_pipeline_user;

-- Grant sequence permissions
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw_data TO data_pipeline_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA processed_data TO data_pipeline_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA analytics TO data_pipeline_user;
