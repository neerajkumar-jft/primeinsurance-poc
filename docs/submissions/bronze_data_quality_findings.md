# Bronze table data quality findings

We examined all 5 Bronze tables before writing any transformation code. Here is what we found.

## customers (3,605 rows from 7 files)

| Issue | Region / File | Details |
|-------|--------------|---------|
| Customer ID column has 3 different names | `CustomerID` in files 1,4,5,6,7; `Customer_ID` in file 2; `cust_id` in file 3 | Need to coalesce into single `customer_id` column |
| Region column has 2 names | `Reg` in customers_1.csv; `Region` in all others | Coalesce into `region` |
| Region uses abbreviations instead of full names | customers_5.csv | `W`, `C`, `E`, `S` instead of West, Central, East, South |
| Marital status column has 2 names | `Marital_status` in files 1,6; `Marital` in files 3,4,5,7 | customers_2 has neither (missing entirely) |
| Education column has 2 names | `Education` in files 1,3,5,6,7; `Edu` in file 2 | customers_4 has neither (missing entirely) |
| City column has 2 names | `City` in files 1,3,4,5,6,7; `City_in_state` in file 2 | Coalesce into `city` |
| Job column missing from file 1 | customers_1.csv | 199 rows with no job data |
| Marital_status and Education columns are swapped | customers_6.csv | `Marital_status` contains education values (primary/secondary/tertiary), `Education` contains marital values (divorced/married/single). Affects all 199 rows |
| Typo in education values | customers_5.csv | `"terto"` appears 73 times, should be `"tertiary"` |
| String "NA" used instead of NULL | customers_1 (11), customers_3 (10), customers_5 (6), customers_7 (79), customers_2 (12) | 118+ rows across Education/Edu columns |
| HHInsurance column missing entirely | customers_2.csv | 200 rows with NULL |
| Education column missing entirely | customers_4.csv | 1,005 rows with NULL |

## claims (1,000 rows from 2 JSON files)

| Issue | Region / File | Details |
|-------|--------------|---------|
| All columns are strings | Both files (claims_1.json, claims_2.json) | Numeric fields (injury, property, vehicle, bodily_injuries, witnesses) stored as strings. Need type casting |
| Date fields are corrupted | Both files, all 1,000 rows | `incident_date`, `Claim_Logged_On`, `Claim_Processed_On` contain values like `"27:00.0"`, `"45:00.0"`. Only the time portion survived; the date part is lost |
| String "NULL" instead of actual null | Both files | `Claim_Processed_On`: 526 rows. `Claim_Logged_On`: 11 rows. `vehicle`: 29 rows |
| "?" placeholder instead of null | Both files | `property_damage`: 360 rows. `police_report_available`: 343 rows. `collision_type`: 178 rows |
| Claim_Rejected values | Both files | Contains `Y` and `N` as expected, no unexpected values |

## policy (1,000 rows from 1 file)

| Issue | Region / File | Details |
|-------|--------------|---------|
| Negative umbrella_limit | policy.csv, 1 row (policy_number 526039) | Value is -1,000,000. All other values are 0 or positive (2M-10M range) |
| Column name misspelling | policy.csv | `policy_deductable` should be `policy_deductible`. We preserve as-is since this is how the source sent it |
| No other issues found | -- | No null policy_numbers, no null customer_ids, no duplicate keys. Premiums range $433-$2,048 (reasonable). policy_csl values are clean: 100/300, 250/500, 500/1000 |

## sales (4,981 rows from 3 files)

| Issue | Region / File | Details |
|-------|--------------|---------|
| 3,132 entirely empty rows (62.9%) | sales_1.csv: 1,877 null rows; Sales_2.csv: 1,254 null rows; sales_4.csv: 1 null row | Every field including sales_id is NULL. These are blank/padding rows from the source files |
| Date columns stored as strings | All 3 files | `ad_placed_on` and `sold_on` are strings in format `dd-MM-yyyy HH:mm`. Need parsing to timestamp |
| Unsold inventory (NULL sold_on) | Across all files | Among valid rows, 162 records have no `sold_on` date (legitimate unsold inventory, not a data error) |

## cars (2,500 rows from 1 file)

| Issue | Region / File | Details |
|-------|--------------|---------|
| Mileage contains unit strings | cars.csv | Values like `"23.4 kmpl"`, `"17.3 km/kg"`. Two different units: `kmpl` (majority) and `km/kg` (24 CNG/LPG vehicles) |
| Engine contains unit strings | cars.csv | Values like `"1248 CC"`. All rows have " CC" suffix |
| Max power contains unit strings | cars.csv | Values like `"74 bhp"`. All rows have " bhp" suffix |
| Torque has inconsistent formats | cars.csv | Mixed formats: `"190Nm@ 2000rpm"`, `"22.4 kgm at 1750-2750rpm"`, `"12.7@ 2,700(kgm@ rpm)"`. Mixed units (Nm vs kgm) and separators |
| No critical issues | -- | No null car_ids, no duplicates, km_driven has no negatives (range 1,000-1,500,000) |

## Severity summary

**Critical (data corruption or loss):**
- Claims date fields are truncated to time-only across all 1,000 rows
- customers_6.csv has Marital_status and Education columns completely swapped
- 62.9% of sales rows are entirely empty

**High (schema inconsistencies requiring harmonization):**
- 3 different customer ID column names across customer files
- Multiple column name variants for region, marital, education, city
- Missing columns in specific files (no job in file 1, no marital in file 2, no education in file 4)
- Claims: all fields typed as strings including numerics and dates

**Medium (data quality issues):**
- Region abbreviations in customers_5 (W/C/E/S)
- "terto" typo in customers_5 (73 rows)
- String "NA", "NULL", "?" used as null placeholders across customers and claims
- 1 negative umbrella_limit in policy

**Low (format/parsing):**
- Cars: unit strings embedded in mileage, engine, max_power, torque
- Sales: date strings need parsing to timestamps
