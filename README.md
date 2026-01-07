## This document demonstrates the execution of snowflake_upload pipeline for the Infrastructure/Workflow Engineering task:
### 1. Setup & Dependencies
  1. Install Python 3.10+
  2. Create a Snowflake Trial Account
  3. Snowflake warehouse, database, schema, and table should be created as per the original README file given in the task repo
  4. Install python dependecies -pip install requirements.txt
  5. Create a config folder in the repo and config/snowflake_config.json file. below is the structure of the .json file that has been created:

          {
            "snowflake_config_obj": {        /// the values passed here should match the details from the snowflake account
              "user": "YOUR_SF_USER",
              "password": "YOUR_SF_PASSWORD",
              "account": "YOUR_SF_ACCOUNT_ID",
              "warehouse": "TEST_WH",
              "database": "CRI_TEST",
              "schema": "PD_DAILY",
              "role": "TEST_ROLE"
            },
          
            "db_params": {
              "host": "localhost",
              "user": "mysql_user",
              "password": "mysql_password",
              "database": "some_mysql_db"
            },
          
            "local_file_path": "C:\\path\\to\\pd_1.mat"  /// this path should contain the destination path for .mat file
            }

  6. Modify the helper.py file as follows:
     a. Fetching and loading the config file with os.path (pre-defined) functions:
         ```
         home_dir = os.path.expanduser("~") 
         config_path = os.path.join(home_dir, "Documents", "CRI_NUS","snowflake_upload", "config", "snowflake_config.json")
         ```
     
     b. Modify the data_preprocessing function as below to convert the .mat file into pd dataframe as required for the snowflake schema:
        def data_preprocessing():
          mat_dataset = loadmat("C:/Users/KAUSHIK M R/Documents/pd_1.mat")
          np_array = mat_dataset["pd"]
      
          df = pd.DataFrame(np_array)
      
          # 1) Names that match the .mat layout
          new_column_names = [
              "comp_id", "yyyy", "mm", "dd",
              "pd_1", "pd_3", "pd_6", "pd_12",
              "pd_24", "pd_36", "pd_48", "pd_60",
          ]
          df.columns = new_column_names
      
          # 2) Cast to integers so we can build a date and comp_id maps to INTEGER
          df["comp_id"] = df["comp_id"].astype("Int64")
          df["yyyy"] = df["yyyy"].astype("Int64")
          df["mm"] = df["mm"].astype("Int64")
          df["dd"] = df["dd"].astype("Int64")
      
          # 3) Build a single Snowflake DATE column called 'date'
          df = df.rename(columns={"yyyy": "year", "mm": "month", "dd": "day"})
          df["date"] = pd.to_datetime(df[["year", "month", "day"]])
          df = df.drop(columns=["year", "month", "day"])
      
          # 4) Reorder / subset columns to match CRI_TEST.PD_DAILY.PD_DAILY_TEST
          df = df[
              [
                  "comp_id",  # INTEGER
                  "date",     # DATE
                  "pd_1",
                  "pd_3",
                  "pd_6",
                  "pd_12",
                  "pd_24",
                  "pd_36",
                  "pd_48",
                  "pd_60",
              ]
          ]
          return df

  8. Modify the daily_upload_sf.py file as follows:
     a. Importing the updated data_preprocessing function from helper.py file:
        ```python
        from src.helper import get_calibration_date, pd_dataframe_2_snowflake_parallel, data_preprocessing
        ```
        
     b. Comment out the existing main functions (having daily upload schedule and manual scirpt run) and replace it with the following function to load the entire data into snowflake table:
        ```python
        if __name__ == "__main__":
        <!-- SIMPLE TEST: .mat -> DataFrame -> CRI_TEST.PD_DAILY.PD_DAILY_TEST -->
        df = data_preprocessing()
        print("Rows in df:", len(df))
        print("DEBUG: starting test insert")
        print("DEBUG: df shape:", df.shape)
        print("DEBUG: df head:\n", df.head())
        <!-- NEW: normalize types so Snowflake can bind them --> 
        df["comp_id"] = df["comp_id"].astype(int)
        df["date"] = pd.to_datetime(df["date"]).dt.date          # pure Python date objects
        for col in ["pd_1", "pd_3", "pd_6", "pd_12", "pd_24", "pd_36", "pd_48", "pd_60"]:
            df[col] = df[col].astype(float)
    
    
        data_tuple = list(df.itertuples(index=False, name=None))
    
        insert_query = """
            INSERT INTO CRI_TEST.PD_DAILY.PD_DAILY_TEST (
                comp_id,
                date,
                pd_1,
                pd_3,
                pd_6,
                pd_12,
                pd_24,
                pd_36,
                pd_48,
                pd_60
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        print("DEBUG: about to insert rows:", len(data_tuple))
        print("DEBUG: sample tuple:", data_tuple if data_tuple else None)
    
        pd_dataframe_2_snowflake_parallel(
            insert_query=insert_query,
            data_tuple=data_tuple,
            batch_size=1000,
            max_workers=20,
        )
        
      
        print("Done inserting into CRI_TEST.PD_DAILY.PD_DAILY_TEST")  
       ```


  ### 2. Running the python file daily_upload_sf.py
    1. After all the above changes are done, files are saved and code is pushed, run the python app/daily_upload_sf.py
    2. Go to the Snowflake and run the following code to observe the table to be loaded with the pd dataframe data:
        -- Create database & schema for testing
        CREATE DATABASE IF NOT EXISTS CRI_TEST;
        CREATE SCHEMA IF NOT EXISTS CRI_TEST.PD_DAILY;
        
        -- Create a warehouse (XS size is fine)
        CREATE WAREHOUSE IF NOT EXISTS TEST_WH
          WITH WAREHOUSE_SIZE = 'XSMALL'
          AUTO_SUSPEND = 60
          AUTO_RESUME = TRUE;
        
        -- Create test table for daily PD upload
        CREATE TABLE IF NOT EXISTS CRI_TEST.PD_DAILY.PD_DAILY_TEST (
            comp_id      INTEGER,
            date         DATE,
            pd_1         FLOAT,
            pd_3         FLOAT,
            pd_6         FLOAT,
            pd_12        FLOAT,
            pd_24        FLOAT,
            pd_36        FLOAT,
            pd_48        FLOAT,
            pd_60        FLOAT
        );
        
        USE ROLE ACCOUNTADMIN;
        
        CREATE ROLE IF NOT EXISTS TEST_ROLE;
        GRANT ROLE TEST_ROLE TO USER MARK42;
        
        GRANT USAGE ON WAREHOUSE TEST_WH TO ROLE TEST_ROLE;
        GRANT ALL ON DATABASE CRI_TEST TO ROLE TEST_ROLE;
        GRANT ALL ON SCHEMA CRI_TEST.PD_DAILY TO ROLE TEST_ROLE;
        
        
        SELECT *
        FROM CRI_TEST.PD_DAILY.PD_DAILY_TEST
        ORDER BY date DESC;
        
    3. We can observe that the snowflake table is loaded from the TEST_ROLE with a table as seen in the below image:
    <img width="1912" height="916" alt="image" src="https://github.com/user-attachments/assets/d703d6c4-0bef-4ce5-80ea-ee507ebd16d3" />

