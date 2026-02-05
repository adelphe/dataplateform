-- Initialize Superset metadata database.
-- This script creates a separate database for Superset metadata storage.
-- Run as the postgres superuser to create the database.

-- Create the superset database if it doesn't exist
SELECT 'CREATE DATABASE superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec

-- Grant privileges to the airflow user on the superset database
GRANT ALL PRIVILEGES ON DATABASE superset TO airflow;
