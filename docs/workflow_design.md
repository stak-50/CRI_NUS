# PD Daily Upload – Workflow and Design (Part 3)

## 1. Goal

The goal is to run a simple daily job that takes a PD `.mat` file, turns it into a clean table, and loads it into a Snowflake PD table. Each run should:
- Read configuration for connections and parameters.
- Transform the raw file into `comp_id`, `date`, and PD horizon columns.
- Insert the rows into Snowflake in a controlled, observable way.

## 2. Daily workflow (step by step)

1. **Read configuration**

   The script reads `snowflake_config.json`, which contains:
   - Snowflake connection details (account, user, warehouse, database, schema, role).
   - `local_file_path` for the PD `.mat` file.
   - Insert settings such as `batch_size` and `max_workers`.

2. **Preprocess PD data**

   The function `data_preprocessing()` in `helper.py`:
   - Reads `local_file_path` from the config.
   - Uses `loadmat` to load the `.mat` file.
   - Builds a pandas DataFrame with columns:
     - `comp_id`, `date`, `pd_1`, `pd_3`, `pd_6`, `pd_12`, `pd_24`, `pd_36`, `pd_48`, `pd_60`.
   - Cleans types so IDs are integers, dates are proper dates, and PD values are floats.

3. **Prepare rows for insert**

   In `daily_upload_sf.py`, `run_pd_daily_upload()`:
   - Calls `data_preprocessing()` to get the DataFrame.
   - Converts each row to a tuple in the same order as the Snowflake table columns.

4. **Batch insert into Snowflake**

   `run_pd_daily_upload()` then:
   - Reads `batch_size` and `max_workers` from the config.
   - Builds an `INSERT` statement for the PD table.
   - Calls `pd_dataframe_2_snowflake_parallel()` to:
     - Split the tuples into batches.
     - Insert batches into Snowflake using a limited number of worker threads.

5. **Logging and result**

   The `if __name__ == "__main__":` block:
   - Configures Python logging with timestamps and log levels.
   - Logs the start of the upload.
   - Calls `run_pd_daily_upload()`.
   - Logs a success message if it finishes or logs the exception if it fails.

## 3. Why this design is stable enough for a daily job

- **Config-driven behavior**

  Paths and performance parameters (`local_file_path`, `batch_size`, `max_workers`) live in JSON instead of the code. Moving between machines or tuning performance only requires editing config, not changing Python files.

- **Single function to run one upload**

  The `run_pd_daily_upload()` function contains the full sequence for one PD upload. This keeps the bottom of the script small and makes it easy to understand what “one run” does.

- **Basic error handling and logging**

  Wrapping the main call in `try/except` and logging both success and failure is enough to see in logs whether last night’s batch ran correctly and, if not, where it failed.

## 4. Future improvements (beyond prototype)

If this were promoted to a production pipeline, the next steps would be:

- **Tests for the transformation**

  Add unit tests around `data_preprocessing()` to check that the output DataFrame always has the same columns and types. This prevents accidental schema changes.

- **More modular structure**

  Split helpers into smaller modules such as:
  - A module for config loading.
  - A module for PD transformation.
  - A module for Snowflake client and insert logic.
  This would make the code easier to reuse and test.

- **Richer handling of batch failures**

  Extend the batch insert helper so it can detect which batch failed, log that batch index and error, and optionally retry or save failed rows for later investigation.

- **Scheduling and file checks**

  In production, the script would be triggered by a scheduler (cron, Airflow, or Snowflake Tasks) and include a small “wait until file exists and is non-empty” step before starting the upload.
