# How Databricks AI/BI Genie helped identify data issues during the Bronze table review

We used Genie during the Bronze table review to speed up the exploratory analysis. Instead of writing SQL manually for each check, we asked Genie questions in plain English against the Bronze tables and it returned the query results directly.

For example, we asked "show me all distinct values in the Region and Reg columns in the customers table grouped by source file" and Genie generated the SQL that revealed customers_5.csv uses single-letter abbreviations (W, C, E, S) while customers_1.csv uses the Reg column name instead of Region.

We also asked "which customer files are missing the Education column" and Genie returned a query showing that customers_4.csv has NULL for Education across all its rows, and customers_2.csv uses Edu instead. Asking "are there any rows where Marital_status contains education-like values" helped us catch the column swap in customers_6.csv, where Marital_status had values like primary, secondary, tertiary instead of married, single, divorced.

For claims, asking "show me sample values for incident_date, Claim_Logged_On, and Claim_Processed_On" immediately surfaced the corrupted date format (27:00.0 instead of real dates). Asking "how many rows have the literal string NULL in Claim_Processed_On" gave us the 526 count quickly.

For sales, asking "how many rows have a null sales_id" revealed the 3,132 empty rows, which was 62.9% of the table. That was not something we expected to find.

Genie was particularly useful for the kind of ad-hoc questions that come up during data profiling, where you're not sure what you're looking for until you see the results. It saved us from writing dozens of one-off SQL queries manually.
