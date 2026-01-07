-- ============================================================
-- ROLE SETUP: SERVICE_ROLE
-- Description: Role used by backend services (e.g., FastAPI)
-- Access: Read/write access to CRI data with a limited-size WH
-- ============================================================

CREATE ROLE SERVICE_ROLE;

-- Grant access to a smaller warehouse used for standard queries
GRANT USAGE ON WAREHOUSE CRI_QUERY_WH TO ROLE SERVICE_ROLE;

-- Grant access to CRI database and all its schemas
GRANT USAGE ON DATABASE CRI TO ROLE SERVICE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE CRI TO ROLE SERVICE_ROLE;

-- Grant SELECT access to all existing tables and dynamic tables
GRANT SELECT ON ALL TABLES IN SCHEMA CRI.PUBLIC TO ROLE SERVICE_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CRI.PUBLIC TO ROLE SERVICE_ROLE;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA CRI.PUBLIC TO ROLE SERVICE_ROLE;

-- Future Tables
GRANT SELECT ON FUTURE TABLES IN SCHEMA CRI.PUBLIC TO ROLE SERVICE_ROLE;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CRI.PUBLIC TO ROLE SERVICE_ROLE;
GRANT INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CRI.PUBLIC TO ROLE SERVICE_ROLE;

-- Grant full DML access (read/write) to all tables in CRI
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CRI.PUBLIC TO ROLE SERVICE_ROLE;

-- ============================================================
-- ROLE SETUP: PIPELINE_ROLE
-- Description: Role used by data pipeline processes (e.g., ETL)
-- Access: Inherits all SERVICE_ROLE permissions + bigger WH
-- ============================================================

CREATE ROLE PIPELINE_ROLE;

-- Grant access to larger warehouses suitable for heavy processing
GRANT USAGE ON WAREHOUSE CRI_PIPELINES_MEDIUM_WH TO ROLE PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE CRI_PIPELINES_XSMALL_WH TO ROLE PIPELINE_ROLE;

-- Existing and future tasks
GRANT OPERATE ON ALL TASKS IN DATABASE CRI TO ROLE PIPELINE_ROLE;
GRANT OPERATE ON FUTURE TASKS IN DATABASE CRI TO ROLE PIPELINE_ROLE;

-- Inherit all permissions from SERVICE_ROLE (object-level access)
GRANT ROLE SERVICE_ROLE TO ROLE PIPELINE_ROLE;

-- ============================================================
-- USER SETUP: CRI_SMART_DATA_BACKEND
-- Description: Service account used by the FastAPI Smart Data backend
-- Access: Assigned to SERVICE_ROLE (limited WH)
-- ============================================================

CREATE OR REPLACE USER CRI_SMART_DATA_BACKEND 
    DEFAULT_WAREHOUSE = 'CRI_QUERY_WH'
    DEFAULT_ROLE = 'SERVICE_ROLE'
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Used for integrating FastAPI Smart Data backend service';

GRANT ROLE SERVICE_ROLE TO USER CRI_SMART_DATA_BACKEND;

-- ============================================================
-- USER SETUP: CRI_SMART_DATA_PIPELINE
-- Description: Service account used by the pipeline/ETL
-- Access: Assigned to PIPELINE_ROLE (bigger WH)
-- ============================================================

CREATE OR REPLACE USER CRI_SMART_DATA_PIPELINE 
    DEFAULT_WAREHOUSE = 'CRI_PIPELINES_XSMALL_WH'
    DEFAULT_ROLE = 'PIPELINE_ROLE'
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Used for integrating pipeline services';

GRANT ROLE PIPELINE_ROLE TO USER CRI_SMART_DATA_PIPELINE;

-- ============================================================
-- AUTHENTICATION POLICY SETUP (if supported)
-- Description: Enable secure and flexible auth for the service user
-- ============================================================
-- (Optional: Only if your Snowflake edition supports authentication policies)
-- CREATE AUTHENTICATION POLICY allow_path_token_policy
--   AUTHENTICATION_METHODS = ('OAUTH', 'PASSWORD', 'PROGRAMMATIC_ACCESS_TOKEN');
-- ALTER USER CRI_SMART_DATA_BACKEND SET AUTHENTICATION POLICY allow_path_token_policy;
-- ALTER USER CRI_SMART_DATA_PIPELINE SET AUTHENTICATION POLICY allow_path_token_policy;

-- ============================================================
-- PAT SETUP: Programmatic Access Token
-- Description: Long-lived token used by backend service (FastAPI) to connect securely
-- Note: Token is restricted to SERVICE_ROLE or PIPELINE_ROLE for scoped access
-- ============================================================
-- (PAT creation is typically done via UI or Snowflake CLI, not SQL. Document the intent here)
-- Example (manual step):
--   Create PAT for CRI_SMART_DATA_BACKEND with SERVICE_ROLE, 1-year expiry
--   Create PAT for CRI_SMART_DATA_PIPELINE with PIPELINE_ROLE, 1-year expiry

-- ============================================================
-- AUDIT & VERIFICATION
-- ============================================================
SHOW GRANTS TO ROLE SERVICE_ROLE;
SHOW GRANTS TO ROLE PIPELINE_ROLE;
SHOW GRANTS TO USER CRI_SMART_DATA_BACKEND;
SHOW GRANTS TO USER CRI_SMART_DATA_PIPELINE;
SHOW USERS; 