-- 01_stages.sql
-- Staging area setup and management

-- Create stage for loading parquet files
CREATE OR REPLACE STAGE CRI.CRI_PROD_MARCUS.pd_load_stage;

-- List files in the stage
LIST @pd_load_stage;

-- Remove all parquet files from the stage (cleanup)
REMOVE @pd_load_stage PATTERN='.*\\.parquet';