CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.employees;

CREATE TABLE bronze.employees (
    employee_id INTEGER PRIMARY KEY,
    last_name VARCHAR(100) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    birth_date DATE NOT NULL,
    business_unit VARCHAR(100) NOT NULL,
    hire_date DATE NOT NULL,
    annual_salary INTEGER NOT NULL,
    contract_type VARCHAR(50) NOT NULL,
    leave_days INTEGER NOT NULL,
    home_address TEXT NOT NULL,
    transport_mode VARCHAR(100) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);