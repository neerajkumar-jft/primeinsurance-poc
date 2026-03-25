# Architectural Decisions

## D1: Pipeline Language — PySpark + SQL Mix
- **Decision**: Use PySpark for complex transformations (customer dedup, anomaly scoring), SQL for simple transforms and aggregations
- **Why**: Team is comfortable with both; PySpark gives more control for ML/AI; SQL is cleaner for Gold aggregations

## D2: Ingestion Strategy — Auto Loader with Schema Evolution
- **Decision**: Use Auto Loader (`cloudFiles`) with `mergeSchema=true` for Bronze ingestion
- **Why**: Handles the schema evolution scenario (new `Birth Date` column), is incremental by default, and satisfies idempotency requirement
- **Alternative considered**: `COPY INTO` — simpler but less robust for schema changes

## D3: Customer ID Unification — Hash-based Master ID
- **Decision**: Generate a deterministic `master_customer_id` using hash of normalized (name + state + city) as matching key, then assign a unified ID
- **Why**: Regional systems use different ID formats; exact match on raw IDs is impossible; fuzzy matching on normalized attributes is the pragmatic approach for a POC

## D4: Quality Rule Enforcement — DLT Expectations + Custom Quarantine
- **Decision**: Use DLT expectations for inline quality checks; write failed records to separate quarantine tables with reason codes
- **Why**: DLT expectations integrate natively with pipeline monitoring; quarantine tables satisfy compliance team's audit trail requirement

## D5: Gold Layer Pattern — Star Schema with Materialized Views
- **Decision**: Classic Kimball star schema with conformed dimensions; use materialized views for frequently-queried aggregations
- **Why**: Star schema is the most queryable pattern for BI; materialized views pre-compute expensive joins for dashboard performance

## D6: LLM Integration — OpenAI SDK + databricks-meta-llama-3.1-70b-instruct
- **Decision**: Use the prescribed model via OpenAI SDK pattern; structured JSON output parsing
- **Why**: Required by hackathon; Llama 3.1 70B is strong for structured generation tasks

## D7: RAG Implementation — FAISS + Manual Chunking
- **Decision**: Convert dim_policy to text → chunk with overlap → embed with Databricks embeddings → FAISS index → retrieve + generate
- **Why**: FAISS is lightweight, no external dependencies; works within Databricks notebook environment

## D8: Anomaly Detection — Rule-based Statistical Scoring (not ML)
- **Decision**: 5 weighted rules producing a 0-100 anomaly score; LLM generates investigation briefs for high-scoring claims
- **Why**: Hackathon explicitly says "statistical methods (z-scores, rule-based thresholds) are reliable"; LLM is for explanation, not detection

## D9: Genie Space — Configured on Gold Layer Only
- **Decision**: Point Genie Space at Gold fact + dimension tables with rich column descriptions
- **Why**: Gold is the curated, business-ready layer; Genie works best with well-described, clean tables

## D10: Repository Structure
- **Decision**: Organize by milestone/layer, not by person
- **Why**: Reviewers evaluate by milestone; clean structure = higher submission score
