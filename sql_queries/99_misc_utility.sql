-- 99_misc_utility.sql
-- Miscellaneous utility queries, DML, and admin commands

-- Example: Truncate user table
TRUNCATE TABLE cri_user;

-- Example: Grant execute task
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

-- Example: Manual task execution
EXECUTE TASK CRI.CRI_PROD_MARCUS.refresh_pd_downstreams_daily;