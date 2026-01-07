-- 02_raw_tables.sql
-- Raw/base tables for all ingested data

-- Company metadata
CREATE OR REPLACE TABLE CRI.CRI_PROD_MARCUS.companies_mapping_info (
  Company_ID int NOT NULL,
  Company_Name varchar(255) DEFAULT NULL,
  Ticker varchar(100) DEFAULT NULL,
  Exchange_ID int DEFAULT 99999,
  Country_Exchange varchar(255) DEFAULT 'Unknown Exchange',
  Domicile_ID int DEFAULT 99999,
  Country_Domicile varchar(255) DEFAULT 'Unknown Domicile',
  Region_ID INT DEFAULT 99999,
  Region VARCHAR(255) DEFAULT 'Unknown Region',
  Industry_Sector_ID int DEFAULT 99,
  Industry_Sector varchar(255) DEFAULT 'Unclassifiable',
  Industry_Group_ID int DEFAULT 9910,
  Industry_Group varchar(255) DEFAULT 'Unclassifiable',
  Industry_Subgroup_ID int DEFAULT 991010,
  Industry_Subgroup varchar(255) DEFAULT 'Unclassifiable',
  Remark text,
  PRIMARY KEY (Company_ID)
);

-- PD data (main)
CREATE OR REPLACE TABLE CRI.CRI_PROD_MARCUS.pd_company_individual_daily_complete_latest (
    data_date DATE NOT NULL,
    company_id NUMBER NOT NULL,
    calibration_date DATE NULL,
    operation_time TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    PDIR FLOAT DEFAULT 0 COMMENT 'Probability of Default Implied Rating',
    PD_1 FLOAT, PD_2 FLOAT, ..., PD_60 FLOAT,
    PRIMARY KEY (data_date, company_id, operation_time)
);

-- AS data
CREATE OR REPLACE TABLE CRI.CRI_PROD_MARCUS.as_company_individual_daily_complete_latest (
    data_date DATE NOT NULL,
    company_id NUMBER NOT NULL,
    calibration_date DATE NULL,
    operation_time TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    "1Y" FLOAT, "2Y" FLOAT, "3Y" FLOAT, "4Y" FLOAT, "5Y" FLOAT
);

-- Excluded companies
CREATE OR REPLACE TABLE CRI.CRI_PROD_MARCUS.excluded_companies (
    region STRING,
    company_id INT,
    country_id INT,
    ticker STRING,
    company_name STRING,
    reason STRING,
    remark STRING,
    start_date DATE,
    end_date DATE,
    error_found_date DATE,
    sql_query STRING,
    stop_reason STRING,
    internal_remarks STRING
);

-- Credit events
CREATE OR REPLACE TABLE CRI.CRI_PROD_MARCUS.credit_events_raw (
    default_date DATE NOT NULL,
    company_id INT,
    default_type INT,
    event_id INT,
    col_extra INT,
    additional_comments STRING NULL,
    updated TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- Event code mapping
CREATE OR REPLACE TABLE CRI.CRI_PROD_MARCUS.event_code_mapping (
    id INT,
    action_name STRING,
    subcategory STRING
);