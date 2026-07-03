CREATE TABLE IF NOT EXISTS bronze.activities (
    activity_id BIGINT PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    sport_type VARCHAR(100) NOT NULL,
    distance_m INTEGER,
    duration_s INTEGER NOT NULL,
    comment TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);