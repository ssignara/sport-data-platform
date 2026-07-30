-- ============================================================
-- CRÉATION DU SCHÉMA ANALYTIQUE
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;


-- ============================================================
-- SUPPRESSION DES ANCIENNES VUES
-- ============================================================
-- Les vues sont supprimées avant leur recréation afin de permettre
-- la modification de leur structure, de leurs colonnes et de leur ordre.
--
-- Les tables bronze.employees, bronze.activities
-- et analytics.parameters ne sont pas supprimées.

DROP VIEW IF EXISTS analytics.global_kpis CASCADE;
DROP VIEW IF EXISTS analytics.business_unit_summary CASCADE;
DROP VIEW IF EXISTS analytics.activities_by_month CASCADE;
DROP VIEW IF EXISTS analytics.activities_by_sport CASCADE;
DROP VIEW IF EXISTS analytics.employee_activity_summary CASCADE;
DROP VIEW IF EXISTS analytics.activities_prepared CASCADE;


-- ============================================================
-- 1. PARAMÈTRES MÉTIER
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.parameters (
    parameter_name VARCHAR(100) PRIMARY KEY,
    parameter_value NUMERIC NOT NULL
);

INSERT INTO analytics.parameters (
    parameter_name,
    parameter_value
)
VALUES
    ('wellbeing_threshold', 15),
    ('wellbeing_days', 5),
    ('bonus_rate', 0.05)
ON CONFLICT (parameter_name)
DO UPDATE SET
    parameter_value = EXCLUDED.parameter_value;


-- ============================================================
-- 2. ACTIVITÉS PRÉPARÉES
-- ============================================================
-- Cette vue constitue la première couche de transformation.
--
-- Elle :
-- - transforme les mètres en kilomètres ;
-- - transforme les secondes en minutes et en heures ;
-- - extrait le jour, le mois et l'année ;
-- - calcule la durée à partir des dates ;
-- - contrôle la cohérence de la durée ;
-- - prépare les données avant leur utilisation dans Metabase.

CREATE OR REPLACE VIEW analytics.activities_prepared AS
SELECT
    a.activity_id,
    a.employee_id,
    a.start_date,
    a.end_date,
    a.sport_type,

    COALESCE(a.distance_m, 0) AS distance_m,

    ROUND(
        COALESCE(a.distance_m, 0)::NUMERIC / 1000,
        2
    ) AS distance_km,

    COALESCE(a.duration_s, 0) AS duration_s,

    ROUND(
        COALESCE(a.duration_s, 0)::NUMERIC / 60,
        2
    ) AS duration_minutes,

    ROUND(
        COALESCE(a.duration_s, 0)::NUMERIC / 3600,
        2
    ) AS duration_hours,

    CASE
        WHEN a.start_date IS NOT NULL
             AND a.end_date IS NOT NULL
        THEN EXTRACT(
            EPOCH FROM (a.end_date - a.start_date)
        )::BIGINT
        ELSE NULL
    END AS calculated_duration_s,

    CASE
        WHEN a.duration_s IS NULL THEN FALSE

        WHEN a.start_date IS NULL
             OR a.end_date IS NULL
        THEN FALSE

        WHEN ABS(
            a.duration_s
            - EXTRACT(
                EPOCH FROM (a.end_date - a.start_date)
            )
        ) <= 60
        THEN TRUE

        ELSE FALSE
    END AS duration_is_consistent,

    DATE(a.start_date) AS activity_date,

    DATE_TRUNC(
        'month',
        a.start_date
    )::DATE AS activity_month,

    EXTRACT(
        YEAR FROM a.start_date
    )::INTEGER AS activity_year,

    EXTRACT(
        MONTH FROM a.start_date
    )::INTEGER AS activity_month_number,

    EXTRACT(
        DOW FROM a.start_date
    )::INTEGER AS activity_day_of_week_number,

    CASE EXTRACT(DOW FROM a.start_date)::INTEGER
        WHEN 0 THEN 'Dimanche'
        WHEN 1 THEN 'Lundi'
        WHEN 2 THEN 'Mardi'
        WHEN 3 THEN 'Mercredi'
        WHEN 4 THEN 'Jeudi'
        WHEN 5 THEN 'Vendredi'
        WHEN 6 THEN 'Samedi'
    END AS activity_day_of_week,

    CASE
        WHEN EXTRACT(
            DOW FROM a.start_date
        )::INTEGER IN (0, 6)
        THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    NULLIF(
        TRIM(a.comment),
        ''
    ) AS comment,

    a.loaded_at

FROM bronze.activities a;


-- ============================================================
-- 3. SYNTHÈSE DES ACTIVITÉS PAR SALARIÉ
-- ============================================================
-- Cette vue :
-- - relie les données RH aux activités ;
-- - calcule les indicateurs sportifs par salarié ;
-- - applique la règle des 15 activités ;
-- - attribue les 5 jours bien-être ;
-- - détermine l'éligibilité à la prime sportive ;
-- - calcule la prime à 5 % du salaire annuel.

CREATE OR REPLACE VIEW analytics.employee_activity_summary AS
SELECT
    e.employee_id,
    e.first_name,
    e.last_name,

    CONCAT(
        e.first_name,
        ' ',
        e.last_name
    ) AS employee_full_name,

    e.birth_date,
    e.business_unit,
    e.hire_date,
    e.annual_salary,
    e.contract_type,
    e.leave_days,
    e.home_address,
    e.transport_mode,

    EXTRACT(
        YEAR FROM AGE(CURRENT_DATE, e.birth_date)
    )::INTEGER AS employee_age,

    EXTRACT(
        YEAR FROM AGE(CURRENT_DATE, e.hire_date)
    )::INTEGER AS seniority_years,

    COUNT(a.activity_id) AS activity_count,

    COUNT(
        DISTINCT a.activity_date
    ) AS active_days_count,

    COUNT(
        DISTINCT a.sport_type
    ) AS distinct_sport_count,

    COALESCE(
        ROUND(
            SUM(a.distance_km),
            2
        ),
        0
    ) AS total_distance_km,

    COALESCE(
        ROUND(
            AVG(a.distance_km),
            2
        ),
        0
    ) AS average_distance_km,

    COALESCE(
        ROUND(
            SUM(a.duration_minutes),
            2
        ),
        0
    ) AS total_duration_minutes,

    COALESCE(
        ROUND(
            SUM(a.duration_hours),
            2
        ),
        0
    ) AS total_duration_hours,

    COALESCE(
        ROUND(
            AVG(a.duration_minutes),
            2
        ),
        0
    ) AS average_duration_minutes,

    MIN(a.activity_date) AS first_activity_date,

    MAX(a.activity_date) AS last_activity_date,

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
        WHEN COUNT(a.activity_id) >= (
            SELECT parameter_value
            FROM analytics.parameters
            WHERE parameter_name = 'wellbeing_threshold'
        )
        THEN (
            SELECT parameter_value
            FROM analytics.parameters
            WHERE parameter_name = 'wellbeing_days'
        )::INTEGER
        ELSE 0
    END AS wellbeing_days_granted,

    CASE
        WHEN COUNT(a.activity_id) >= (
            SELECT parameter_value
            FROM analytics.parameters
            WHERE parameter_name = 'wellbeing_threshold'
        )
        THEN 0

        ELSE (
            SELECT parameter_value
            FROM analytics.parameters
            WHERE parameter_name = 'wellbeing_threshold'
        )::INTEGER - COUNT(a.activity_id)::INTEGER
    END AS activities_remaining_for_wellbeing,

    CASE
        WHEN LOWER(TRIM(e.transport_mode)) IN (
            LOWER('Marche/running'),
            LOWER('Vélo/Trottinette/Autres')
        )
        THEN TRUE
        ELSE FALSE
    END AS eligible_sport_bonus,

    CASE
        WHEN LOWER(TRIM(e.transport_mode)) IN (
            LOWER('Marche/running'),
            LOWER('Vélo/Trottinette/Autres')
        )
        THEN ROUND(
            e.annual_salary::NUMERIC * (
                SELECT parameter_value
                FROM analytics.parameters
                WHERE parameter_name = 'bonus_rate'
            ),
            2
        )
        ELSE 0
    END AS bonus_amount

FROM bronze.employees e

LEFT JOIN analytics.activities_prepared a
    ON e.employee_id = a.employee_id

GROUP BY
    e.employee_id,
    e.first_name,
    e.last_name,
    e.birth_date,
    e.business_unit,
    e.hire_date,
    e.annual_salary,
    e.contract_type,
    e.leave_days,
    e.home_address,
    e.transport_mode;


-- ============================================================
-- 4. INDICATEURS PAR TYPE DE SPORT
-- ============================================================

CREATE OR REPLACE VIEW analytics.activities_by_sport AS
SELECT
    sport_type,

    COUNT(*) AS activity_count,

    COUNT(
        DISTINCT employee_id
    ) AS active_employees,

    ROUND(
        COALESCE(
            SUM(distance_km),
            0
        ),
        2
    ) AS total_distance_km,

    ROUND(
        COALESCE(
            AVG(distance_km),
            0
        ),
        2
    ) AS average_distance_km,

    ROUND(
        COALESCE(
            SUM(duration_minutes),
            0
        ),
        2
    ) AS total_duration_minutes,

    ROUND(
        COALESCE(
            SUM(duration_hours),
            0
        ),
        2
    ) AS total_duration_hours,

    ROUND(
        COALESCE(
            AVG(duration_minutes),
            0
        ),
        2
    ) AS average_duration_minutes,

    MIN(activity_date) AS first_activity_date,

    MAX(activity_date) AS last_activity_date

FROM analytics.activities_prepared

GROUP BY sport_type;


-- ============================================================
-- 5. INDICATEURS PAR MOIS
-- ============================================================

CREATE OR REPLACE VIEW analytics.activities_by_month AS
SELECT
    activity_month,
    activity_year,
    activity_month_number,

    COUNT(*) AS activity_count,

    COUNT(
        DISTINCT employee_id
    ) AS active_employees,

    COUNT(
        DISTINCT sport_type
    ) AS distinct_sport_count,

    ROUND(
        COALESCE(
            SUM(distance_km),
            0
        ),
        2
    ) AS total_distance_km,

    ROUND(
        COALESCE(
            AVG(distance_km),
            0
        ),
        2
    ) AS average_distance_km,

    ROUND(
        COALESCE(
            SUM(duration_minutes),
            0
        ),
        2
    ) AS total_duration_minutes,

    ROUND(
        COALESCE(
            SUM(duration_hours),
            0
        ),
        2
    ) AS total_duration_hours,

    ROUND(
        COALESCE(
            AVG(duration_minutes),
            0
        ),
        2
    ) AS average_duration_minutes

FROM analytics.activities_prepared

GROUP BY
    activity_month,
    activity_year,
    activity_month_number

ORDER BY activity_month;


-- ============================================================
-- 6. INDICATEURS PAR BUSINESS UNIT
-- ============================================================

CREATE OR REPLACE VIEW analytics.business_unit_summary AS
SELECT
    business_unit,

    COUNT(*) AS employee_count,

    SUM(
        CASE
            WHEN activity_count > 0
            THEN 1
            ELSE 0
        END
    ) AS active_employees,

    SUM(activity_count) AS total_activities,

    ROUND(
        COALESCE(
            SUM(total_distance_km),
            0
        ),
        2
    ) AS total_distance_km,

    ROUND(
        COALESCE(
            SUM(total_duration_hours),
            0
        ),
        2
    ) AS total_duration_hours,

    ROUND(
        COALESCE(
            AVG(activity_count),
            0
        ),
        2
    ) AS average_activities_per_employee,

    SUM(
        CASE
            WHEN eligible_wellbeing_days
            THEN 1
            ELSE 0
        END
    ) AS employees_eligible_wellbeing,

    SUM(
        wellbeing_days_granted
    ) AS total_wellbeing_days_granted,

    SUM(
        activities_remaining_for_wellbeing
    ) AS total_activities_remaining_for_wellbeing,

    SUM(
        CASE
            WHEN eligible_sport_bonus
            THEN 1
            ELSE 0
        END
    ) AS employees_eligible_bonus,

    ROUND(
        COALESCE(
            SUM(bonus_amount),
            0
        ),
        2
    ) AS total_bonus_cost

FROM analytics.employee_activity_summary

GROUP BY business_unit;


-- ============================================================
-- 7. KPI GLOBAUX
-- ============================================================
-- Cette vue fournit directement les valeurs des cartes KPI
-- utilisées dans Metabase.

CREATE OR REPLACE VIEW analytics.global_kpis AS
SELECT
    COUNT(*) AS total_employees,

    SUM(
        CASE
            WHEN activity_count > 0
            THEN 1
            ELSE 0
        END
    ) AS active_employees,

    COUNT(*) - SUM(
        CASE
            WHEN activity_count > 0
            THEN 1
            ELSE 0
        END
    ) AS inactive_employees,

    SUM(activity_count) AS total_activities,

    ROUND(
        COALESCE(
            SUM(total_distance_km),
            0
        ),
        2
    ) AS total_distance_km,

    ROUND(
        COALESCE(
            SUM(total_duration_minutes),
            0
        ),
        2
    ) AS total_duration_minutes,

    ROUND(
        COALESCE(
            SUM(total_duration_hours),
            0
        ),
        2
    ) AS total_duration_hours,

    SUM(
        CASE
            WHEN eligible_wellbeing_days
            THEN 1
            ELSE 0
        END
    ) AS employees_eligible_wellbeing,

    SUM(
        wellbeing_days_granted
    ) AS total_wellbeing_days_granted,

    SUM(
        activities_remaining_for_wellbeing
    ) AS total_activities_remaining_for_wellbeing,

    SUM(
        CASE
            WHEN eligible_sport_bonus
            THEN 1
            ELSE 0
        END
    ) AS employees_eligible_bonus,

    ROUND(
        COALESCE(
            SUM(bonus_amount),
            0
        ),
        2
    ) AS total_bonus_cost,

    ROUND(
        COALESCE(
            AVG(bonus_amount)
                FILTER (
                    WHERE eligible_sport_bonus
                ),
            0
        ),
        2
    ) AS average_bonus_amount,

    ROUND(
        COALESCE(
            AVG(activity_count)
                FILTER (
                    WHERE activity_count > 0
                ),
            0
        ),
        2
    ) AS average_activities_per_active_employee,

    ROUND(
        COALESCE(
            AVG(total_distance_km)
                FILTER (
                    WHERE activity_count > 0
                ),
            0
        ),
        2
    ) AS average_distance_per_active_employee_km,

    ROUND(
        COALESCE(
            (
                SUM(
                    CASE
                        WHEN activity_count > 0
                        THEN 1
                        ELSE 0
                    END
                )::NUMERIC
                / NULLIF(COUNT(*), 0)
            ) * 100,
            0
        ),
        2
    ) AS employee_participation_rate_percent,

    ROUND(
        COALESCE(
            (
                SUM(
                    CASE
                        WHEN eligible_wellbeing_days
                        THEN 1
                        ELSE 0
                    END
                )::NUMERIC
                / NULLIF(COUNT(*), 0)
            ) * 100,
            0
        ),
        2
    ) AS wellbeing_eligibility_rate_percent,

    ROUND(
        COALESCE(
            (
                SUM(
                    CASE
                        WHEN eligible_sport_bonus
                        THEN 1
                        ELSE 0
                    END
                )::NUMERIC
                / NULLIF(COUNT(*), 0)
            ) * 100,
            0
        ),
        2
    ) AS sport_bonus_eligibility_rate_percent

FROM analytics.employee_activity_summary;