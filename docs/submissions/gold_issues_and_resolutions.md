# Gold layer -- issues and resolutions

## Issue 1: policy_id vs policy_number mismatch

When joining fact_claims to dim_policy, the Silver claims table calls the field `policy_id` (from the source JSON key `PolicyID`), but the Silver policy table calls it `policy_number`. These are the same value but named differently.

Resolution: in the Gold fact_claims table definition, we alias `policy_id` to `policy_number` so the join key matches dim_policy:
```python
F.col("policy_id").alias("policy_number")
```

## Issue 2: claim amounts contain nulls

Some claim records have null values in injury_amount, property_amount, or vehicle_amount (from the string "NULL" values in the source JSON that Silver converted to actual nulls). When calculating total_claim_amount as the sum of all three, null + any number = null in Spark.

Resolution: we use `F.coalesce(col, F.lit(0))` for each component before summing:
```python
(F.coalesce(F.col("injury_amount"), F.lit(0)) +
 F.coalesce(F.col("property_amount"), F.lit(0)) +
 F.coalesce(F.col("vehicle_amount"), F.lit(0))).alias("total_claim_amount")
```

## Issue 3: customer deduplication

Bronze has 3,605 customer rows from 7 files, but many customers appear in multiple files (customers_7.csv alone has 1,604 rows that overlap with other files). Loading all 3,605 into dim_customer would create duplicates.

Resolution: we deduplicate on customer_id using a window function, keeping the first occurrence by load timestamp:
```python
w = Window.partitionBy("customer_id").orderBy("_load_timestamp")
df = df.withColumn("_row_num", F.row_number().over(w))
df = df.filter(F.col("_row_num") == 1)
```
Result: 1,604 unique customers in dim_customer.

## Issue 4: corrupted claim dates block processing time metric

The assignment asks for "average claim processing time" as a Gold metric. The date fields (claim_logged_on, claim_processed_on) are corrupted at source with values like "27:00.0". We cannot calculate processing_time_days.

Resolution: we include the raw date strings in fact_claims for completeness and document the limitation. The mv_claims_by_severity view reports rejection rate and claim value by severity instead of processing time. This was flagged in the Bronze review, logged in dq_issues, and noted in the dimensional model design document.

## Issue 5: days_listed changes daily for unsold inventory

For unsold cars (sold_on IS NULL), days_listed is calculated as `datediff(current_date(), ad_placed_on)`. This means the value changes every day even without new data.

Resolution: the materialized view captures a snapshot when the pipeline runs. This is acceptable because the pipeline runs daily. The value reflects "days listed as of the last pipeline run" rather than real time, which is sufficient for the morning standup dashboard.
