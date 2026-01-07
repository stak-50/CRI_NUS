-- 03_dynamic_tables.sql
-- Dynamic tables for analytics-ready, joined, and deduplicated data

-- PD dynamic table (latest per company/date)
CREATE OR REPLACE TRANSIENT DYNAMIC TABLE CRI.CRI_PROD_MARCUS.dt_pd_company_individual_daily_complete_latest
TARGET_LAG = DOWNSTREAM
WAREHOUSE = 'CRI_PIPELINES_MEDIUM_WH'
CLUSTER BY (data_date, company_id, region_id, domicile_id, industry_sector_id)
AS
SELECT
    p.data_date,
    p.company_id,
    c.Company_Name,
    c.ticker,
    c.domicile_id,
    c.region_id,
    c.industry_sector_id,
    c.industry_group_id,
    c.industry_subgroup_id,
    p.operation_time,
    p.PDIR,
    p.PD_1, p.PD_2, ..., p.PD_60
FROM (
    SELECT *
    FROM CRI.CRI_PROD_MARCUS.pd_company_individual_daily_complete_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY data_date, company_id ORDER BY operation_time DESC) = 1
) p
INNER JOIN companies_mapping_info c
    ON p.company_id = c.company_id;

-- (Add other dynamic tables as in your docs)
-- PD DATA (only latest 2 days)
CREATE OR REPLACE TRANSIENT DYNAMIC TABLE CRI.CRI_PROD_MARCUS.dt_pd_company_individual_daily_complete_latest_2d
TARGET_LAG = DOWNSTREAM
WAREHOUSE = 'CRI_PIPELINES_XSMALL_WH'
AS
SELECT *
FROM CRI.CRI_PROD_MARCUS.dt_pd_company_individual_daily_complete_latest
WHERE data_date IN (
    SELECT DISTINCT data_date
    FROM CRI.CRI_PROD_MARCUS.dt_pd_company_individual_daily_complete_latest
    ORDER BY data_date DESC
    LIMIT 2
);