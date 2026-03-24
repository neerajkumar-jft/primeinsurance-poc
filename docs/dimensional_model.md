# PrimeInsurance gold layer - Dimensional model

## Fact tables

### fact_claims

Grain: one row per claim

| Column | Type | Description |
|--------|------|-------------|
| claim_id | INT | Degenerate dimension |
| policy_key | INT | FK to dim_policies |
| customer_key | INT | FK to dim_customers (resolved via policy) |
| car_key | INT | FK to dim_cars (resolved via policy) |
| incident_date_key | INT | FK to dim_date |
| logged_date_key | INT | FK to dim_date |
| injury | DOUBLE | Injury claim amount |
| property_amount | DOUBLE | Property damage amount |
| vehicle | DOUBLE | Vehicle damage amount |
| total_claim_amount | DOUBLE | injury + property_amount + vehicle |
| days_to_process | INT | DATEDIFF(claim_processed_on, claim_logged_on) |
| is_rejected | BOOLEAN | TRUE if claim_rejected = 'Y' |
| incident_type | STRING | Type of incident |
| incident_severity | STRING | Severity level |
| collision_type | STRING | Collision type |
| incident_state | STRING | State where incident occurred |
| incident_city | STRING | City where incident occurred |
| authorities_contacted | STRING | Authority contacted |
| number_of_vehicles_involved | INT | Vehicle count |
| property_damage | STRING | YES/NO |
| bodily_injuries | INT | Injury count |
| witnesses | INT | Witness count |
| police_report_available | STRING | YES/NO |

Source: silver.claims_cleaned JOIN silver.policy_cleaned (for customer_id, car_id) JOIN gold dimensions (for surrogate keys)

### fact_sales

Grain: one row per sales listing (includes unsold inventory)

| Column | Type | Description |
|--------|------|-------------|
| sales_id | INT | Degenerate dimension |
| car_key | INT | FK to dim_cars |
| date_key | INT | FK to dim_date (ad placed date) |
| sold_date_key | INT | FK to dim_date (sale date, NULL if unsold) |
| original_selling_price | DOUBLE | Listed price |
| days_on_market | INT | Days between listing and sale (or listing and today if unsold) |
| is_sold | BOOLEAN | TRUE if car sold |
| region | STRING | Listing region |
| state | STRING | Listing state |
| city | STRING | Listing city |
| seller_type | STRING | Individual/Dealer |
| owner | STRING | Ownership history |

Source: silver.sales_cleaned JOIN gold.dim_cars (for car_key)

---

## Dimension tables

### dim_customers

Source: silver.customers_unified (deduplicated)

| Column | Type | Description |
|--------|------|-------------|
| customer_key | INT | Surrogate PK |
| customer_id | INT | Business key |
| region | STRING | Full region name |
| state | STRING | US state |
| city | STRING | City |
| job | STRING | Occupation |
| education | STRING | Education level |
| marital_status | STRING | Marital status |
| default_flag | INT | Credit default (0/1) |
| balance | INT | Account balance |
| hh_insurance | INT | Household insurance (0/1) |
| car_loan | INT | Car loan indicator (0/1) |

### dim_policies

Source: silver.policy_cleaned

| Column | Type | Description |
|--------|------|-------------|
| policy_key | INT | Surrogate PK |
| policy_number | INT | Business key |
| policy_bind_date | DATE | Date policy bound |
| policy_state | STRING | Policy state |
| policy_csl | STRING | Combined single limit (e.g. 100/300) |
| policy_deductible | INT | Deductible amount |
| policy_annual_premium | DOUBLE | Annual premium |
| umbrella_limit | INT | Umbrella coverage |

### dim_cars

Source: silver.cars_cleaned

| Column | Type | Description |
|--------|------|-------------|
| car_key | INT | Surrogate PK |
| car_id | INT | Business key |
| name | STRING | Full vehicle name |
| km_driven | INT | Odometer |
| fuel | STRING | Fuel type |
| transmission | STRING | Manual/Automatic |
| mileage_kmpl | DOUBLE | Fuel efficiency |
| engine_cc | INT | Engine displacement |
| max_power_bhp | DOUBLE | Max power |
| torque_nm | DOUBLE | Torque in Nm |
| torque_rpm | INT | RPM at peak torque |
| seats | INT | Seat count |
| model | STRING | Model identifier |

Shared dimension: referenced by both fact_claims (via policy->car) and fact_sales (directly).

### dim_date

Source: generated (SEQUENCE from 2010-01-01 to 2030-12-31)

| Column | Type | Description |
|--------|------|-------------|
| date_key | INT | Surrogate PK, format YYYYMMDD |
| full_date | DATE | ISO date |
| year | INT | Calendar year |
| quarter | INT | Quarter (1-4) |
| month | INT | Month (1-12) |
| month_name | STRING | January, February, etc. |
| day_of_week | INT | Day number |
| day_name | STRING | Monday, Tuesday, etc. |
| is_weekend | BOOLEAN | TRUE if Sat/Sun |

Role-playing: used multiple times per fact table (incident_date_key, logged_date_key on claims; date_key, sold_date_key on sales).

---

## How the model answers each business failure

### 1. Regulatory pressure (12% inflated customer count)

dim_customers is built from customers_unified, which already resolved the duplicate IDs between customers_1 and customers_7 in silver. COUNT(DISTINCT customer_key) gives the accurate count. vw_regulatory_customer_count provides the auditable breakdown by region with a TOTAL row.

### 2. Claims backlog (18 days vs 7-day benchmark)

fact_claims.days_to_process = DATEDIFF(claim_processed_on, claim_logged_on). Average processing time can be sliced by region (via dim_customers), severity, policy type (via dim_policies). vw_claims_processing flags the percentage exceeding 7 days by region and severity.

### 3. Revenue leakage (aging unsold inventory)

fact_sales.days_on_market uses current_date() for unsold inventory, so it updates every pipeline run. vw_revenue_leakage groups by model and region, surfacing which models are stuck where, with at_risk_revenue summing listed prices of unsold cars past 60 days.

---

## Business views

| View | Answers |
|------|---------|
| vw_claims_processing | Avg processing time by region/severity, % exceeding 7-day benchmark |
| vw_regulatory_customer_count | Deduplicated customer count by region |
| vw_revenue_leakage | Unsold inventory > 60 days by model/region, at-risk revenue |
| vw_claims_by_policy_type | Rejection rate and loss ratio by policy CSL tier and region |

---

## Join paths

| Query need | Join path | Keys |
|-----------|-----------|------|
| Claims to customers | fact_claims -> dim_customers | customer_key |
| Claims to policies | fact_claims -> dim_policies | policy_key |
| Claims to cars | fact_claims -> dim_cars | car_key |
| Claims to dates | fact_claims -> dim_date | incident_date_key / logged_date_key |
| Sales to cars | fact_sales -> dim_cars | car_key |
| Sales to dates | fact_sales -> dim_date | date_key / sold_date_key |

All joins use surrogate integer keys. Business keys (customer_id, policy_number, car_id) are attributes on the dimensions.
