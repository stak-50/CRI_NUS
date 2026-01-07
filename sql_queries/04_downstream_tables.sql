-- 04_downstream_tables.sql
-- Downstream/website-facing dynamic tables

-- Example: Website PD data
CREATE OR REPLACE TRANSIENT DYNAMIC TABLE CRI.CRI_WEBSITE_DATA.dt_pd_company_website_data
TARGET_LAG = DOWNSTREAM
WAREHOUSE = 'CRI_PIPELINES_XSMALL_WH'
AS
SELECT 
    pd.company_id AS CompanyID,
    pd.data_date AS DTDate,
    pd.data_date AS FSDate,
    pd.PD_1 AS DFT1,
    pd.PD_3 AS DFT2,
    pd.PD_6 AS DFT3,
    pd.PD_12 AS DFT4,
    pd.PD_24 AS DFT5,
    pd.PD_36 AS DFT6,
    pd.PD_60 AS DFT7,
    pd.region_id AS RegionFlag,
    0 AS Draft,
    CURRENT_TIMESTAMP() AS Updated
FROM CRI.CRI_PROD_MARCUS.dt_pd_company_individual_daily_complete_latest pd
WHERE NOT EXISTS (
    SELECT 1
    FROM CRI.CRI_PROD_MARCUS.excluded_companies ex
    WHERE pd.company_id = ex.company_id
    AND pd.data_date >= ex.start_date
    AND pd.data_date <= COALESCE(ex.end_date, CURRENT_DATE)
);

-- (Add other downstream tables as in your docs)