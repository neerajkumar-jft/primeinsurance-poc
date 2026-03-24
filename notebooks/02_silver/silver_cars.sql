-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver - Cars standardization
-- MAGIC The main problem here is units jammed into numeric fields.
-- MAGIC Mileage has "23.4 kmpl", engine has "1248 CC", max_power has "74 bhp",
-- MAGIC and torque has formats like "190Nm@ 2000rpm".
-- MAGIC
-- MAGIC We extract the numeric values and split torque into separate
-- MAGIC torque_nm and torque_rpm columns.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW cars_cleaned (
  CONSTRAINT valid_car_id EXPECT (car_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_km EXPECT (km_driven >= 0),
  CONSTRAINT valid_fuel EXPECT (fuel IN ('Diesel', 'Petrol', 'LPG', 'CNG', 'Electric')),
  CONSTRAINT valid_seats EXPECT (seats BETWEEN 2 AND 10)
)
COMMENT 'Vehicle catalog with numeric values extracted from unit-embedded strings.'
AS
SELECT
  CAST(car_id AS INT) AS car_id,
  name,
  CAST(km_driven AS INT) AS km_driven,
  fuel,
  transmission,

  -- "23.4 kmpl" -> 23.4
  CAST(REGEXP_EXTRACT(mileage, '([\\d.]+)', 1) AS DOUBLE) AS mileage_kmpl,

  -- "1248 CC" -> 1248
  CAST(REGEXP_EXTRACT(engine, '(\\d+)', 1) AS INT) AS engine_cc,

  -- "74 bhp" -> 74.0
  CAST(REGEXP_EXTRACT(max_power, '([\\d.]+)', 1) AS DOUBLE) AS max_power_bhp,

  -- Torque Nm: "190Nm@ 2000rpm" -> 190. Convert kgm to Nm where needed.
  CASE
    WHEN UPPER(torque) LIKE '%NM%' THEN
      CAST(REGEXP_EXTRACT(torque, '([\\d.]+)', 1) AS DOUBLE)
    WHEN LOWER(torque) LIKE '%kgm%' THEN
      ROUND(CAST(REGEXP_EXTRACT(torque, '([\\d.]+)', 1) AS DOUBLE) * 9.80665, 2)
    ELSE CAST(REGEXP_EXTRACT(torque, '([\\d.]+)', 1) AS DOUBLE)
  END AS torque_nm,

  -- Torque RPM: extract number after @
  CAST(REGEXP_REPLACE(
    REGEXP_EXTRACT(torque, '@\\s*([\\d,]+)', 1),
    ',', ''
  ) AS INT) AS torque_rpm,

  CAST(seats AS INT) AS seats,
  model,
  _source_file,
  _ingestion_timestamp
FROM prime_insurance_jellsinki_poc.bronze.raw_cars;
