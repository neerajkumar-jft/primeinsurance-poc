-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver - Claims cleaning
-- MAGIC Two problems with this data: the dates are broken and several fields
-- MAGIC use placeholder values instead of nulls.
-- MAGIC
-- MAGIC The date fields (incident_date, Claim_Logged_On, Claim_Processed_On) have
-- MAGIC values like "27:00.0" which are Excel serial date fragments. The number
-- MAGIC before the colon is days since 1899-12-30. We parse that back to a date.
-- MAGIC
-- MAGIC property_damage and police_report_available use "?" instead of null.
-- MAGIC Claim_Processed_On has the literal string "NULL" for rejected claims.
-- MAGIC All numeric fields came in as strings from JSON.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW claims_cleaned (
  CONSTRAINT valid_claim_id EXPECT (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_policy_id EXPECT (policy_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_incident_date EXPECT (incident_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_claim_rejected EXPECT (claim_rejected IN ('Y', 'N')) ON VIOLATION DROP ROW,
  CONSTRAINT positive_injury EXPECT (injury >= 0),
  CONSTRAINT positive_property_amt EXPECT (property_amount >= 0),
  CONSTRAINT positive_vehicle EXPECT (vehicle >= 0),
  CONSTRAINT valid_num_vehicles EXPECT (number_of_vehicles_involved > 0)
)
COMMENT 'Claims with parsed dates, corrected nulls, and proper types.'
AS
SELECT
  CAST(ClaimID AS INT) AS claim_id,
  CAST(PolicyID AS INT) AS policy_id,

  -- Parse Excel serial date fragments: "27:00.0" -> 27 days after 1899-12-30
  CASE
    WHEN incident_date IS NULL OR TRIM(incident_date) IN ('NULL', '') THEN NULL
    ELSE DATEADD(DAY, CAST(SUBSTRING(incident_date, 1, INSTR(incident_date, ':') - 1) AS INT), DATE'1899-12-30')
  END AS incident_date,

  incident_state,
  incident_city,
  incident_location,
  incident_type,
  NULLIF(collision_type, '?') AS collision_type,
  incident_severity,
  authorities_contacted,

  CAST(number_of_vehicles_involved AS INT) AS number_of_vehicles_involved,
  CASE WHEN property_damage = '?' THEN NULL ELSE property_damage END AS property_damage,
  CAST(bodily_injuries AS INT) AS bodily_injuries,
  CAST(witnesses AS INT) AS witnesses,
  CASE WHEN police_report_available = '?' THEN NULL ELSE police_report_available END AS police_report_available,

  CAST(injury AS DOUBLE) AS injury,
  CAST(property AS DOUBLE) AS property_amount,
  CAST(vehicle AS DOUBLE) AS vehicle,

  Claim_Rejected AS claim_rejected,

  CASE
    WHEN Claim_Logged_On IS NULL OR TRIM(Claim_Logged_On) IN ('NULL', '') THEN NULL
    ELSE DATEADD(DAY, CAST(SUBSTRING(Claim_Logged_On, 1, INSTR(Claim_Logged_On, ':') - 1) AS INT), DATE'1899-12-30')
  END AS claim_logged_on,

  CASE
    WHEN Claim_Processed_On IS NULL OR TRIM(Claim_Processed_On) IN ('NULL', '') THEN NULL
    ELSE DATEADD(DAY, CAST(SUBSTRING(Claim_Processed_On, 1, INSTR(Claim_Processed_On, ':') - 1) AS INT), DATE'1899-12-30')
  END AS claim_processed_on,

  _source_file,
  _ingestion_timestamp
FROM prime_insurance_jellsinki_poc.bronze.raw_claims;

-- COMMAND ----------

-- Quarantine: claims that failed validation

CREATE OR REFRESH MATERIALIZED VIEW quarantine_claims
COMMENT 'Claims that failed validation. Preserved with original values for investigation.'
AS
SELECT
  ClaimID AS claim_id,
  PolicyID AS policy_id,
  incident_date AS raw_incident_date,
  Claim_Logged_On AS raw_claim_logged_on,
  Claim_Processed_On AS raw_claim_processed_on,
  incident_state,
  incident_city,
  property_damage,
  police_report_available,
  Claim_Rejected AS claim_rejected,
  _source_file,
  _ingestion_timestamp,
  CASE
    WHEN ClaimID IS NULL THEN 'null_claim_id'
    WHEN PolicyID IS NULL THEN 'null_policy_id'
    WHEN incident_date IS NULL OR TRIM(incident_date) IN ('NULL', '') THEN 'null_incident_date'
    WHEN Claim_Rejected NOT IN ('Y', 'N') THEN 'invalid_claim_rejected'
    ELSE 'validation_failure'
  END AS quarantine_reason,
  current_timestamp() AS quarantined_at
FROM prime_insurance_jellsinki_poc.bronze.raw_claims
WHERE
  ClaimID IS NULL
  OR PolicyID IS NULL
  OR incident_date IS NULL OR TRIM(incident_date) IN ('NULL', '')
  OR Claim_Rejected NOT IN ('Y', 'N');
