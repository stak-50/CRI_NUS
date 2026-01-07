-- 06_user_auth_tables.sql
-- User and portfolio management tables

CREATE OR REPLACE TABLE cri_user (
    id STRING DEFAULT UUID_STRING(),
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    company VARCHAR(255),
    designation VARCHAR(255),
    department VARCHAR(750),
    industry VARCHAR(255) DEFAULT '',
    phone VARCHAR(31),
    type VARCHAR(15) DEFAULT 'REGULAR',
    status VARCHAR(15) DEFAULT 'UNACTIVATED',
    activation_code VARCHAR(127),
    additional_comments TEXT,
    global_company_access BOOLEAN DEFAULT FALSE,
    clients_access TEXT,
    second_level_agreement BOOLEAN DEFAULT FALSE,
    hash_password VARCHAR(255) NOT NULL,
    hash_token VARCHAR(255) NOT NULL,
    created TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    receive_occasional BOOLEAN DEFAULT FALSE,
    receive_periodic BOOLEAN DEFAULT FALSE,
    cri_dashboard_access BOOLEAN DEFAULT FALSE,
    buda_access BOOLEAN DEFAULT FALSE,
    special_remarks TEXT,
    expiry_date TIMESTAMP_NTZ,
    buda_popup_disable BOOLEAN DEFAULT FALSE,
    last_login TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TABLE cri_user_portfolio (
    id STRING DEFAULT UUID_STRING(),
    user_id STRING,
    portfolio_name STRING,
    portfolio_info BINARY,
    created TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);