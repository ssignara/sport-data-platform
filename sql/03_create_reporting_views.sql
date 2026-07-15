CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.parameters (
    parameter_name VARCHAR(100) PRIMARY KEY,
    parameter_value NUMERIC NOT NULL
);

INSERT INTO analytics.parameters (parameter_name, parameter_value)
VALUES
    ('bonus_rate', 0.05),
    ('wellbeing_threshold', 15)
ON CONFLICT (parameter_name)
DO NOTHING;

CREATE OR REPLACE VIEW analytics.employee_activity_summary AS
SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    e.business_unit,
    e.annual_salary,
    e.transport_mode,
    COUNT(a.activity_id) AS activity_count,
    COALESCE(SUM(a.distance_m), 0) AS total_distance_m,
    COALESCE(SUM(a.duration_s), 0) AS total_duration_s,
    CASE
        WHEN COUNT(a.activity_id) >= (
            SELECT parameter_value
            FROM analytics.parameters
            WHERE parameter_name = 'wellbeing_threshold'
        )
        THEN TRUE
        ELSE FALSE
    END AS eligible_wellbeing_days,
    CASE
        WHEN e.transport_mode IN ('Marche/running', 'Vélo/Trottinette/Autres')
        THEN TRUE
        ELSE FALSE
    END AS eligible_sport_bonus,
    CASE
        WHEN e.transport_mode IN ('Marche/running', 'Vélo/Trottinette/Autres')
        THEN e.annual_salary * (
            SELECT parameter_value
            FROM analytics.parameters
            WHERE parameter_name = 'bonus_rate'
        )
        ELSE 0
    END AS bonus_amount
FROM bronze.employees e
LEFT JOIN bronze.activities a
    ON e.employee_id = a.employee_id
GROUP BY
    e.employee_id,
    e.first_name,
    e.last_name,
    e.business_unit,
    e.annual_salary,
    e.transport_mode;

CREATE OR REPLACE VIEW analytics.activities_by_sport AS
SELECT
    sport_type,
    COUNT(*) AS activity_count,
    COALESCE(SUM(distance_m), 0) AS total_distance_m,
    COALESCE(SUM(duration_s), 0) AS total_duration_s
FROM bronze.activities
GROUP BY sport_type;

CREATE OR REPLACE VIEW analytics.activities_by_month AS
SELECT
    DATE_TRUNC('month', start_date) AS activity_month,
    COUNT(*) AS activity_count,
    COUNT(DISTINCT employee_id) AS active_employees
FROM bronze.activities
GROUP BY DATE_TRUNC('month', start_date)
ORDER BY activity_month;

CREATE OR REPLACE VIEW analytics.global_kpis AS
SELECT
    COUNT(DISTINCT employee_id) AS total_employees,
    SUM(activity_count) AS total_activities,
    SUM(total_distance_m) AS total_distance_m,
    SUM(CASE WHEN eligible_wellbeing_days THEN 1 ELSE 0 END) AS employees_eligible_wellbeing,
    SUM(CASE WHEN eligible_sport_bonus THEN 1 ELSE 0 END) AS employees_eligible_bonus,
    SUM(bonus_amount) AS total_bonus_cost
FROM analytics.employee_activity_summary;