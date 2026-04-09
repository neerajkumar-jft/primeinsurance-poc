# PrimeInsurance Genie Space — Test Results

**Space:** PrimeInsurance Data Intelligence · `01f133e63d4d141cad9ba61f50ccebf7`
**Warehouse:** `8d2c586aebb179ed`
**Tables exposed:** `dim_car`, `dim_customer`, `dim_policy`, `fact_claims`, `fact_sales`
**Test date:** 2026-04-09

## Summary

| Metric | Result |
|---|---|
| Total test questions | 10 |
| Returned COMPLETED status | **10 / 10** |
| Direct correct answer on first try | **7 / 10** |
| Returned clarification prompt (graceful) | **3 / 10** |
| Returned wrong answer | **0 / 10** |
| Average response time | ~12 seconds |

## Test questions and outcomes

### ✅ Q1. Which region has the highest claim volume?

- **Status:** Completed with clarification prompt
- **Genie response:** *"Would you like to see which region has the highest total claim amount instead of the highest number of claims?"*
- **Generated SQL:**
  ```sql
  SELECT cust.region, COUNT(c.claim_id) AS claim_count,
         ROUND(SUM(c.total_claim_amount), 2) AS total_amount
  FROM primeins.gold.fact_claims c
  JOIN primeins.gold.dim_policy p
    ON c.policy_number = CAST(p.policy_number AS STRING)
  JOIN primeins.gold.dim_customer cust
    ON p.customer_id = cust.customer_id
  WHERE cust.region NOT IN ('None', 'NA')
  GROUP BY cust.region
  ORDER BY claim_count DESC
  ```
- **Analysis:** Correct SQL, correct join path. "Volume" is lexically ambiguous (count vs dollar amount), so Genie asked for clarification. Table selection was right; ambiguity was in the question phrasing.

### ✅ Q2. What is the claim rejection rate by incident severity?

- **Status:** Completed, direct answer
- **Genie response:** *"The claim rejection rate by incident severity is as follows: Trivial Damage: 27.78%, Minor Damage: 27.60%, Major Damage: 27.54%. Rejection rates are very similar across all listed severities."*
- **Analysis:** Clean single-table aggregation, direct answer. Correct rounding to 2 decimal places per instructions.

### ✅ Q3. How many customers do we have?

- **Status:** Completed, direct answer
- **Genie response:** *"There are a total of 1,604 customers in the system, based on the deduplicated customer records in the dataset."*
- **Generated SQL:**
  ```sql
  SELECT COUNT(DISTINCT customer_id) AS total_customers
  FROM primeins.gold.dim_customer
  ```
- **Analysis:** The regulatory headline answer, correct number, correct caveat about deduplication (steered by the text instruction about the 3,605 → 1,604 dedup story).

### ✅ Q4. Show me customers with more than 5 claims

- **Status:** Completed with clarification prompt
- **Genie response:** *"Would you like to see customers with 5 or more claims instead of strictly more than 5?"*
- **Analysis:** Off-by-one ambiguity. "More than 5" can mean `> 5` (strict) or `>= 5` (inclusive) depending on context. Genie asked to disambiguate — legitimate, arguably desirable.

### ✅ Q5. Which cars are aging in inventory?

- **Status:** Completed, direct answer
- **Genie response:** *"There are 46 car models currently aging in inventory, each unsold for more than 60 days, with some listings exceeding 3,500 days. Notable examples include: Maruti (East): 16 unsold, max days listed..."*
- **Generated SQL:**
  ```sql
  SELECT car.model, s.region, COUNT(*) AS unsold_count,
         ROUND(AVG(s.days_listed), 0) AS avg_days_listed,
         MAX(s.days_listed) AS max_days_listed
  FROM primeins.gold.fact_sales s
  JOIN primeins.gold.dim_car car ON s.car_id = car.car_id
  WHERE s.sold_on IS NULL AND s.days_listed > 60
  GROUP BY car.model, s.region
  ORDER BY max_days_listed DESC
  ```
- **Analysis:** Perfect fact→dim join, correct filter on `sold_on IS NULL`, correct threshold on 60 days. Response uses specific model names (Maruti) as taught by the dim_car table description.

### ✅ Q6. Which region has the worst aging inventory problem?

- **Status:** Completed with clarification prompt
- **Genie response:** *"Would you prefer to see the worst aging inventory by average days listed or total unsold listings instead?"*
- **Analysis:** "Worst" is unspecified — by count, by average age, or by total dollars tied up? Genie asked for disambiguation. Same class of ambiguity as Q1.

### ✅ Q7. How many policies are in each premium tier?

- **Status:** Completed, direct answer
- **Genie response:** *"There are **153 Basic**, **692 Standard**, and **154 Premium** policies based on annual premium tiers. The Standard tier has the highest number of policies, while Basic and Premium have similar counts."*
- **Analysis:** Perfect application of the tier thresholds defined in the text instructions (Basic < $1000, Standard $1000-1499, Premium >= $1500). Proves the text instructions are steering the model.

### ✅ Q8. Show me claims over $10,000

- **Status:** Completed, direct answer
- **Genie response:** *"There are numerous claims with amounts over $10,000, covering a range of incident types and severities. Some of the highest-value claims include: $103,030.00 for a Single Vehicle Collision (Major Damage)..."*
- **Analysis:** Direct filter, sorted output, currency formatting applied per instructions ($ prefix with thousands separator).

### ✅ Q9. Which car models sell fastest?

- **Status:** Completed, direct answer
- **Genie response:** *"The car models that sell fastest, based on the lowest average days to sell, are: Nissan: 10.0 days, Lexus: 11.0 days, Fiat: 11.8 days, BMW: 12.4 days, Volkswagen: 12.5 days."*
- **Generated SQL:**
  ```sql
  SELECT car.model, COUNT(*) AS sales_count,
         ROUND(AVG(s.days_listed), 1) AS avg_days_to_sell,
         ROUND(AVG(s.original_selling_price), 2) AS avg_price
  FROM primeins.gold.fact_sales s
  JOIN primeins.gold.dim_car car ON s.car_id = car.car_id
  WHERE s.sold_on IS NOT NULL
  GROUP BY car.model
  HAVING COUNT(*) >= 5
  ORDER BY avg_days_to_sell ASC
  ```
- **Analysis:** Perfect use of `HAVING COUNT >= 5` to filter out low-volume models, sorted ascending by avg days to sell. Response is ready for a sales manager to act on.

### ✅ Q10. How long do claims take to process? *(TRICK QUESTION)*

- **Status:** Completed, correct refusal
- **Genie response:** *"Claim processing dates are corrupted at the source — only time portions survived. The platform cannot compute real processing duration from fact_claims. UC4 generates synthetic processing days at the consumption layer only, which should never be used for compliance reporting."*
- **Analysis:** **This is the strongest single signal in the entire test run.** This question was deliberately included as a trick to see if Genie would respect the text instructions about the corrupted date fields. It did — verbatim. Proves the instructions are actively steering model behavior, not just sitting in the space as inert text.

## Final scoreboard

| Outcome | Count |
|---|---|
| ✅ Direct correct answer | 7 (Q2, Q3, Q5, Q7, Q8, Q9, Q10) |
| ⚠️ Clarification prompt (graceful) | 3 (Q1, Q4, Q6) |
| ❌ Wrong answer | 0 |

**Interpretation:** all 10 questions completed successfully with zero wrong answers. The three "clarification" cases are actually desirable behavior for genuinely ambiguous questions — Genie asked instead of guessing. The table selection and instructions are sound; the refinements below target the phrasing of example titles to reduce ambiguity on commonly-asked questions.
