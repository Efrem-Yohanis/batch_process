-- Create users and databases
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER campaign_user WITH PASSWORD 'campaign_pass';
CREATE DATABASE campaign_db OWNER campaign_user;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE campaign_db TO campaign_user;
