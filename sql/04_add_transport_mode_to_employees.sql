-- ============================================================
-- MISE À JOUR DE LA TABLE RH EXISTANTE
-- ============================================================
-- Cette migration ajoute les colonnes présentes dans le fichier RH
-- mais absentes de la table PostgreSQL actuellement utilisée.

ALTER TABLE bronze.employees
ADD COLUMN IF NOT EXISTS home_address TEXT;

ALTER TABLE bronze.employees
ADD COLUMN IF NOT EXISTS transport_mode VARCHAR(100);

ALTER TABLE bronze.employees
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;