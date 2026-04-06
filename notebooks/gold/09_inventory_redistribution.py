# Databricks notebook source
# MAGIC %md
# MAGIC # Inventory Redistribution Recommendations
# MAGIC
# MAGIC PrimeInsurance loses 15-20% of revenue from unsold cars that age unnoticed.
# MAGIC A regional sales manager has 34 cars sitting unsold for 90+ days, but their
# MAGIC spreadsheet only covers their region. The same car models sold out in another
# MAGIC region within 2 weeks.
# MAGIC
# MAGIC This report identifies redistribution opportunities: models sitting unsold
# MAGIC in one region that are selling quickly in another.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inventory health overview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_listings,
# MAGIC   SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold,
# MAGIC   SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold,
# MAGIC   ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct,
# MAGIC   ROUND(AVG(CASE WHEN is_sold THEN days_listed END), 0) as avg_days_to_sell,
# MAGIC   SUM(CASE WHEN NOT is_sold AND days_listed > 60 THEN 1 ELSE 0 END) as aging_60_plus,
# MAGIC   SUM(CASE WHEN NOT is_sold AND days_listed > 90 THEN 1 ELSE 0 END) as aging_90_plus,
# MAGIC   ROUND(SUM(CASE WHEN NOT is_sold THEN original_selling_price ELSE 0 END), 0) as unsold_value
# MAGIC FROM primeins.gold.fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sell-through rate by region
# MAGIC Which regions are moving inventory and which aren't?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   COUNT(*) as total_listings,
# MAGIC   SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold,
# MAGIC   SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold,
# MAGIC   ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct,
# MAGIC   ROUND(AVG(CASE WHEN is_sold THEN days_listed END), 0) as avg_days_to_sell,
# MAGIC   SUM(CASE WHEN NOT is_sold AND days_listed > 60 THEN 1 ELSE 0 END) as aging_60_plus,
# MAGIC   ROUND(SUM(CASE WHEN NOT is_sold THEN original_selling_price ELSE 0 END), 0) as unsold_value
# MAGIC FROM primeins.gold.fact_sales
# MAGIC GROUP BY region
# MAGIC ORDER BY sell_through_pct ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Redistribution opportunities
# MAGIC Models sitting unsold in one region but selling quickly in another.
# MAGIC These are the cars that should be moved.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH unsold AS (
# MAGIC   SELECT
# MAGIC     dc.model,
# MAGIC     fs.region,
# MAGIC     COUNT(*) as unsold_count,
# MAGIC     AVG(fs.days_listed) as avg_days_unsold,
# MAGIC     AVG(fs.original_selling_price) as avg_price
# MAGIC   FROM primeins.gold.fact_sales fs
# MAGIC   JOIN primeins.gold.dim_car dc ON fs.car_id = dc.car_id
# MAGIC   WHERE fs.is_sold = false AND fs.days_listed > 60
# MAGIC   GROUP BY dc.model, fs.region
# MAGIC ),
# MAGIC sold_fast AS (
# MAGIC   SELECT
# MAGIC     dc.model,
# MAGIC     fs.region,
# MAGIC     COUNT(*) as sold_count,
# MAGIC     AVG(fs.days_listed) as avg_days_to_sell
# MAGIC   FROM primeins.gold.fact_sales fs
# MAGIC   JOIN primeins.gold.dim_car dc ON fs.car_id = dc.car_id
# MAGIC   WHERE fs.is_sold = true AND fs.days_listed < 30
# MAGIC   GROUP BY dc.model, fs.region
# MAGIC )
# MAGIC SELECT
# MAGIC   u.model,
# MAGIC   u.region as stuck_in_region,
# MAGIC   u.unsold_count as units_stuck,
# MAGIC   CAST(u.avg_days_unsold AS INT) as avg_days_sitting,
# MAGIC   s.region as sells_fast_in_region,
# MAGIC   s.sold_count as units_sold_fast,
# MAGIC   CAST(s.avg_days_to_sell AS INT) as avg_days_to_sell,
# MAGIC   ROUND(u.avg_price, 0) as avg_price,
# MAGIC   ROUND(u.unsold_count * u.avg_price, 0) as revenue_at_risk
# MAGIC FROM unsold u
# MAGIC JOIN sold_fast s ON u.model = s.model AND u.region != s.region
# MAGIC ORDER BY revenue_at_risk DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aging inventory — models sitting longest

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.model,
# MAGIC   dc.name as car_name,
# MAGIC   fs.region,
# MAGIC   fs.days_listed,
# MAGIC   ROUND(fs.original_selling_price, 0) as price,
# MAGIC   dc.fuel,
# MAGIC   dc.transmission,
# MAGIC   dc.km_driven
# MAGIC FROM primeins.gold.fact_sales fs
# MAGIC JOIN primeins.gold.dim_car dc ON fs.car_id = dc.car_id
# MAGIC WHERE fs.is_sold = false
# MAGIC ORDER BY fs.days_listed DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Model performance comparison across regions
# MAGIC Same model, different regions — where does it sell and where does it sit?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.model,
# MAGIC   fs.region,
# MAGIC   COUNT(*) as total_listings,
# MAGIC   SUM(CASE WHEN fs.is_sold THEN 1 ELSE 0 END) as sold,
# MAGIC   SUM(CASE WHEN NOT fs.is_sold THEN 1 ELSE 0 END) as unsold,
# MAGIC   ROUND(SUM(CASE WHEN fs.is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct,
# MAGIC   ROUND(AVG(CASE WHEN fs.is_sold THEN fs.days_listed END), 0) as avg_days_to_sell,
# MAGIC   ROUND(AVG(fs.original_selling_price), 0) as avg_price
# MAGIC FROM primeins.gold.fact_sales fs
# MAGIC JOIN primeins.gold.dim_car dc ON fs.car_id = dc.car_id
# MAGIC GROUP BY dc.model, fs.region
# MAGIC HAVING COUNT(*) >= 3
# MAGIC ORDER BY dc.model, sell_through_pct ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Revenue at risk — unsold inventory value by region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold_units,
# MAGIC   ROUND(SUM(CASE WHEN NOT is_sold THEN original_selling_price ELSE 0 END), 0) as unsold_value,
# MAGIC   ROUND(AVG(CASE WHEN NOT is_sold THEN original_selling_price END), 0) as avg_unsold_price,
# MAGIC   SUM(CASE WHEN NOT is_sold AND days_listed > 90 THEN 1 ELSE 0 END) as critical_aging
# MAGIC FROM primeins.gold.fact_sales
# MAGIC GROUP BY region
# MAGIC ORDER BY unsold_value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Price vs days listed — are we pricing too high?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN original_selling_price < 200000 THEN 'Under 2L'
# MAGIC     WHEN original_selling_price < 500000 THEN '2L - 5L'
# MAGIC     WHEN original_selling_price < 1000000 THEN '5L - 10L'
# MAGIC     ELSE 'Above 10L'
# MAGIC   END as price_band,
# MAGIC   COUNT(*) as total,
# MAGIC   SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold,
# MAGIC   ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct,
# MAGIC   ROUND(AVG(CASE WHEN is_sold THEN days_listed END), 0) as avg_days_to_sell,
# MAGIC   SUM(CASE WHEN NOT is_sold AND days_listed > 60 THEN 1 ELSE 0 END) as aging_unsold
# MAGIC FROM primeins.gold.fact_sales
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Fuel type demand by region
# MAGIC Are certain fuel types harder to sell in specific regions?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.fuel,
# MAGIC   fs.region,
# MAGIC   COUNT(*) as listings,
# MAGIC   SUM(CASE WHEN fs.is_sold THEN 1 ELSE 0 END) as sold,
# MAGIC   ROUND(SUM(CASE WHEN fs.is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct
# MAGIC FROM primeins.gold.fact_sales fs
# MAGIC JOIN primeins.gold.dim_car dc ON fs.car_id = dc.car_id
# MAGIC GROUP BY dc.fuel, fs.region
# MAGIC HAVING COUNT(*) >= 3
# MAGIC ORDER BY dc.fuel, sell_through_pct ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — action items for the sales team
# MAGIC
# MAGIC | Action | Detail |
# MAGIC |--------|--------|
# MAGIC | Redistribute aging stock | Models sitting 60+ days in one region that sell fast in another (see section 3) |
# MAGIC | Review pricing | Check if high-price-band cars have lower sell-through (see section 7) |
# MAGIC | Regional fuel demand | Match fuel type supply to regional demand patterns (see section 8) |
# MAGIC | Weekly aging review | Flag any car crossing 60 days unsold for immediate action |
# MAGIC | Cross-regional visibility | All regions should see consolidated inventory, not just their own spreadsheet |
