# PrimeInsurance POC

Insurance data pipeline using Databricks - Medallion Architecture (Bronze/Silver/Gold) with Gen AI intelligence layer.

## Project Structure
```
notebooks/     - Databricks notebooks (pipeline code)
data/          - Source data files (4 regional systems)
docs/          - Documentation and submission answers
deploy/        - Deployment configuration
```

## Setup
1. Clone this repo to Databricks workspace
2. Run `notebooks/00_setup.py` to initialize catalog and upload data
3. Run pipeline via Databricks Workflow job
