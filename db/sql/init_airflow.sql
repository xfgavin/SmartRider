CREATE USER smartrider_airflow WITH PASSWORD 'smartrider' CREATEDB;
ALTER ROLE  smartrider_airflow SET search_path = airflow;
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO smartrider_airflow;
CREATE SCHEMA airflow;
