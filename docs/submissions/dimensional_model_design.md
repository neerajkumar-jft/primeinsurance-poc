# Dimensional model design -- PrimeInsurance

## Overview

Star schema with 2 fact tables and 4 dimension tables in `primeins.gold`. Built from cleaned Silver tables. Each fact table records one business event. Each dimension provides context for slicing and grouping.

## Fact tables

### fact_claims

One row per claim. This is the table behind the claims backlog and regulatory problems.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| claim_id | string | silver.claims.claim_id | PK |
| policy_number | string | silver.claims.policy_id | FK to dim_policy. Named policy_id in Silver claims, maps to policy_number in dim_policy |
| incident_date | string | silver.claims.incident_date | Corrupted at source, kept as string |
| incident_state | string | silver.claims.incident_state | |
| incident_city | string | silver.claims.incident_city | |
| incident_type | string | silver.claims.incident_type | |
| collision_type | string | silver.claims.collision_type | |
| incident_severity | string | silver.claims.incident_severity | Grain for avg processing time query |
| injury_amount | double | silver.claims.injury_amount | |
| property_amount | double | silver.claims.property_amount | |
| vehicle_amount | double | silver.claims.vehicle_amount | |
| total_claim_amount | double | calculated | injury + property + vehicle |
| vehicles_involved | int | silver.claims.vehicles_involved | |
| bodily_injuries | int | silver.claims.bodily_injuries | |
| witnesses | int | silver.claims.witnesses | |
| authorities_contacted | string | silver.claims.authorities_contacted | |
| property_damage | string | silver.claims.property_damage | |
| police_report_available | string | silver.claims.police_report_available | |
| is_rejected | boolean | silver.claims.claim_rejected | Y -> true, N -> false |
| claim_logged_on | string | silver.claims.claim_logged_on | Corrupted date, kept as string |
| claim_processed_on | string | silver.claims.claim_processed_on | Corrupted date, kept as string |
| _source_file | string | silver.claims._source_file | Lineage |

Grain: one claim event.

Business questions answered:
- Rejection rate by policy type: JOIN dim_policy ON policy_number, GROUP BY policy_csl
- Avg processing time by severity: GROUP BY incident_severity (note: dates are corrupted, so processing_time_days cannot be calculated from the available data)
- Total claim value by severity: GROUP BY incident_severity, SUM(total_claim_amount)
- Claim volume by region: JOIN dim_policy -> dim_customer, GROUP BY region

### fact_sales

One row per car listing. This is the table behind the revenue leakage problem.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| sales_id | int | silver.sales.sales_id | PK |
| car_id | int | silver.sales.car_id | FK to dim_car |
| ad_placed_on | timestamp | silver.sales.ad_placed_on | When the listing was created |
| sold_on | timestamp | silver.sales.sold_on | NULL = unsold inventory |
| original_selling_price | double | silver.sales.original_selling_price | |
| days_listed | int | calculated | Days between ad_placed_on and sold_on (or current_date if unsold) |
| is_sold | boolean | calculated | sold_on IS NOT NULL |
| region | string | silver.sales.region | |
| state | string | silver.sales.state | |
| city | string | silver.sales.city | |
| seller_type | string | silver.sales.seller_type | |
| owner | string | silver.sales.owner | |
| _source_file | string | silver.sales._source_file | Lineage |

Grain: one car listing event.

Business questions answered:
- Unsold cars by model: JOIN dim_car ON car_id, WHERE is_sold = false, GROUP BY model
- Cars sitting 60+ days: WHERE is_sold = false AND days_listed > 60
- Revenue by region: GROUP BY region, SUM(original_selling_price) WHERE is_sold = true

## Dimension tables

### dim_customer

| Column | Type | Source |
|--------|------|--------|
| customer_id | int | silver.customers.customer_id | PK |
| region | string | silver.customers.region |
| state | string | silver.customers.state |
| city | string | silver.customers.city |
| job | string | silver.customers.job |
| marital | string | silver.customers.marital |
| education | string | silver.customers.education |
| default_flag | int | silver.customers.default_flag |
| balance | int | silver.customers.balance |
| hh_insurance | int | silver.customers.hh_insurance |
| car_loan | int | silver.customers.car_loan |

Source: silver.customers. Deduplicated on customer_id (if duplicates exist, take first occurrence).

Used by: fact_claims (via dim_policy.customer_id)

### dim_policy

| Column | Type | Source |
|--------|------|--------|
| policy_number | string | silver.policy.policy_number | PK |
| policy_bind_date | date | silver.policy.policy_bind_date |
| policy_state | string | silver.policy.policy_state |
| policy_csl | string | silver.policy.policy_csl |
| policy_deductible | int | silver.policy.policy_deductible |
| policy_annual_premium | double | silver.policy.policy_annual_premium |
| umbrella_limit | int | silver.policy.umbrella_limit |
| car_id | int | silver.policy.car_id | FK to dim_car |
| customer_id | int | silver.policy.customer_id | FK to dim_customer |

Source: silver.policy.

This dimension connects customers to claims. Claims reference policy_number (as policy_id), and policy carries customer_id and car_id. To get customer information for a claim: fact_claims -> dim_policy (on policy_number) -> dim_customer (on customer_id).

### dim_car

| Column | Type | Source |
|--------|------|--------|
| car_id | int | silver.cars.car_id | PK |
| name | string | silver.cars.name |
| model | string | silver.cars.model |
| fuel | string | silver.cars.fuel |
| transmission | string | silver.cars.transmission |
| km_driven | int | silver.cars.km_driven |
| mileage | double | silver.cars.mileage |
| mileage_unit | string | silver.cars.mileage_unit |
| engine | int | silver.cars.engine |
| max_power | double | silver.cars.max_power |
| torque | string | silver.cars.torque |
| seats | int | silver.cars.seats |

Source: silver.cars.

Bridges the insurance side (policies, claims) and business side (sales). Both dim_policy and fact_sales reference car_id.

### dim_region

| Column | Type | Source |
|--------|------|--------|
| region | string | hardcoded | PK |
| region_code | string | hardcoded |

5 rows: East (E), West (W), Central (C), South (S), North (N).

Small lookup table for grouping. Used across both fact tables.

## Join paths for the 4 business questions

**"Which policy type has the highest claim rejection rate?"**

```sql
SELECT dp.policy_csl,
       COUNT(CASE WHEN fc.is_rejected THEN 1 END) as rejected,
       COUNT(*) as total,
       COUNT(CASE WHEN fc.is_rejected THEN 1 END) * 100.0 / COUNT(*) as rejection_rate
FROM primeins.gold.fact_claims fc
JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
GROUP BY dp.policy_csl
ORDER BY rejection_rate DESC
```

Tables: fact_claims + dim_policy. Join key: policy_number.

**"What is the average claim processing time by incident severity?"**

Note: the date fields (claim_logged_on, claim_processed_on) are corrupted at source. Only time portions survived ("27:00.0"). We cannot calculate actual processing time in days from this data. We include the fields in fact_claims for completeness but the processing time metric cannot be computed reliably.

What we can answer: claim volume and rejection rate by severity.

```sql
SELECT fc.incident_severity,
       COUNT(*) as total_claims,
       COUNT(CASE WHEN fc.is_rejected THEN 1 END) as rejected,
       AVG(fc.total_claim_amount) as avg_claim_value
FROM primeins.gold.fact_claims fc
GROUP BY fc.incident_severity
```

Tables: fact_claims only. No join needed.

**"Which car models are sitting unsold for more than 60 days?"**

```sql
SELECT dc.model, dc.name, fs.days_listed, fs.original_selling_price
FROM primeins.gold.fact_sales fs
JOIN primeins.gold.dim_car dc ON fs.car_id = dc.car_id
WHERE fs.is_sold = false AND fs.days_listed > 60
ORDER BY fs.days_listed DESC
```

Tables: fact_sales + dim_car. Join key: car_id.

**"How does claim volume compare across regions month over month?"**

```sql
SELECT dc.region,
       COUNT(*) as claim_count,
       SUM(fc.total_claim_amount) as total_value
FROM primeins.gold.fact_claims fc
JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
JOIN primeins.gold.dim_customer dc ON dp.customer_id = dc.customer_id
GROUP BY dc.region
```

Tables: fact_claims + dim_policy + dim_customer. Join path: fact_claims -> dim_policy (policy_number) -> dim_customer (customer_id). Two hops instead of chaining five Silver tables.

Note: month over month comparison is limited because claim dates are corrupted. We can show total volume by region but not time-series trends.

## How this model answers PrimeInsurance's three business failures

**Regulatory pressure (customer identity):** dim_customer is the deduplicated, harmonized customer table. Auditable count by region comes from: `SELECT region, COUNT(DISTINCT customer_id) FROM dim_customer GROUP BY region`.

**Claims backlog:** fact_claims has rejection status, severity, amounts, and the join to dim_policy for coverage type analysis. Processing time calculation is blocked by corrupted date fields in the source data.

**Revenue leakage:** fact_sales has days_listed and is_sold flag. Unsold inventory by model and region is a direct query on fact_sales joined to dim_car.

## What the model cannot answer

Processing time in days: the source claims data has corrupted date fields (only time portions like "27:00.0" survived). This was flagged in the Bronze review and logged in dq_issues. The fields are preserved in fact_claims but any metric based on actual calendar days between logging and processing cannot be computed from this data.

Monthly trends: without reliable dates in claims, month over month comparisons are not possible for claims. Sales data has proper timestamps so inventory aging trends are available.
