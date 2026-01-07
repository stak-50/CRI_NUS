from scipy.io import loadmat
import numpy as np
from decimal import Decimal
from datetime import datetime, timedelta
import os, glob, argparse, time, atexit, shutil
import mysql.connector
import h5py  # use to read matlab v7.3 file
import pandas as pd
import re
import json
import os
import snowflake.connector
from src.utils import APP_DIR

home_dir = os.path.expanduser("~") 
config_path = os.path.join(home_dir, "Documents", "CRI_NUS","snowflake_upload", "config", "snowflake_config.json")
#config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
print(f"config_path: {config_path}")
with open(config_path) as f:
    config = json.load(f)

snowflake_config_obj = config["snowflake_config_obj"]
db_params = config["db_params"]
local_file_path = config["local_file_path"]
snowflake_config_obj = config["snowflake_config_obj"]


def create_mysql_table():
    with open(os.path.join("sql_script", "create_table.sql"), "r") as file:
        sql_script = file.read()
        execute_sql_query(sql_script, operation_type="change")
        print("finished creating table")


def create_tmp_folder(folder_name="tmp"):
    # Check if the folder exists
    if not os.path.exists(os.path.join(APP_DIR, folder_name)):
        # Create the folder if it doesn't exist
        os.makedirs(os.path.join(APP_DIR, folder_name))


def cleanup_temp_folder(folder_name="tmp"):
    if os.path.exists(os.path.join(APP_DIR, folder_name)):
        shutil.rmtree(os.path.join(APP_DIR, folder_name))
    os.makedirs(os.path.join(APP_DIR, folder_name))


def get_file_list(snowflake_config_obj, local_mount_path):
    try:
        print(snowflake_config_obj["shared_folder"], local_mount_path.lstrip("/"))
        # List all files/folders in the given path
        full_path = os.path.join(
            snowflake_config_obj["shared_folder"], local_mount_path.lstrip("/")
        )
        print(snowflake_config_obj["shared_folder"], full_path)
        filelist_return = os.listdir(full_path)
    except Exception as e:
        print(f"An error occurred: {e}")
        filelist_return = []

    return filelist_return


def connect_and_download_file(
    snowflake_config_obj, file_list, local_file_path, remote_path, only_one_file_expected
):
    copied_files = []

    if (
        only_one_file_expected == 1 and len(file_list) == 1
    ) or only_one_file_expected == 0:
        for file_name in file_list:
            try:
                source_file = os.path.join(
                    snowflake_config_obj["shared_folder"], remote_path.lstrip("/"), file_name
                )
                destination_file = os.path.join(local_file_path, file_name)

                shutil.copy2(source_file, destination_file)
                copied_files.append(destination_file)
            except Exception as e:
                print(f"An error occurred while copying {file_name}: {e}")
        return copied_files
    else:
        return "Error: only_one_file_expected is set but multiple files provided."


def calculate_pdir(df: pd.DataFrame) -> pd.Series:
    """
    Calculates PDIR values based on 10-day rolling average of PD_12 for each company.
    """

    # Actual rating thresholds (descending)
    rating_thresholds = [
        1.00000000e00, 2.36429705e-01, 1.84359218e-01, 1.24100491e-01, 8.68296884e-02, 
        5.56660292e-02, 3.77365625e-02, 2.38300479e-02, 1.57466890e-02, 1.03831160e-02, 
        6.67894949e-03, 4.23448222e-03, 2.79851727e-03, 1.78383466e-03, 1.15980161e-03, 
        7.47792278e-04, 4.80311462e-04, 3.06114740e-04, 2.08283436e-04, 1.24841915e-04, 
        9.05170934e-05, 0.0,
    ]
    # Corresponding rating levels (22 to 1)
    rating_levels = list(range(22, 0, -1))
    try:
        # Ensure dates are in datetime and sorted properly
        df = df.sort_values(by=["company_id", "date"]).reset_index(drop=True)

        pdir_values = []

        for _, group in df.groupby("company_id"):
            avg_pd = group["PD_12"].rolling(window=10, min_periods=1).mean().values

            for val in avg_pd:
                val = 0.0 if pd.isna(val) else val
                # Map average PD to rating level (PDIR)
                level = next(
                    (
                        lvl
                        for t, lvl in zip(rating_thresholds, rating_levels)
                        if val >= t
                    ),
                    1,
                )
                pdir_values.append(level)

        return pd.Series(pdir_values, index=df.index)
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.Series([])


# def data_preprocessing():
#     mat_dataset = sio.loadmat("C:/Users/KAUSHIK M R/Downloads/pd_1.mat")
#     np_array = mat_dataset['pd'] # This assumes 'pd' is the key in your .mat file
    
#     df = pd.DataFrame(np_array)

#     new_column_names = ['company_id', 'yyyy', 'mm','dd','pd_1','pd_3','pd_6','pd_12','pd_24','pd_36','pd_48','pd_60']
#     df.columns = new_column_names
    
#     # Cast necessary columns to integer types before creating date
#     df['company_id'] = df['company_id'].astype('Int64')
#     df['yyyy'] = df['yyyy'].astype('Int64')
#     df['mm'] = df['mm'].astype('Int64')
#     df['dd'] = df['dd'].astype('Int64')

#     df = df.rename(columns={'yyyy': 'year', 'mm': 'month', 'dd': 'day'})
#     df['date_column'] = pd.to_datetime(df[['year', 'month', 'day']])
#     df = df.drop(columns=['year', 'month', 'day'])

#     print(f"Finished processing DataFrame with {len(df)} rows.")
#     return df
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



def pd_dataframe_2_mysql(insert_query, data: tuple, batch_size=1000):
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor()

    total_rows = len(data)
    for i in range(0, total_rows, batch_size):
        batch_data = data[i : i + batch_size]

        cursor.executemany(insert_query, batch_data)
        conn.commit()
        # print(f"Inserted {len(batch_data)} rows")

    # print("All data inserted successfully")
    
    cursor.close()
    conn.close()


def get_sf_connection():
    sf_conn = snowflake.connector.connect(
        user=snowflake_config_obj["user"],
        password=snowflake_config_obj["password"],
        role=snowflake_config_obj["role"],
        account=snowflake_config_obj["account"],
        warehouse=snowflake_config_obj["warehouse"],
        database=snowflake_config_obj["database"],
        schema=snowflake_config_obj["schema"],
    )

    return sf_conn


import concurrent.futures
from functools import partial
import random
import string


def sf_insert_batch(insert_query, batch, batch_num):
    try:
        sf_conn = get_sf_connection()
        cursor = sf_conn.cursor()
        cursor.executemany(insert_query, batch)
        sf_conn.close()
        print(f"[Batch {batch_num}] Inserted {len(batch)} records")
    except Exception as e:
        print(f"[Batch {batch_num}] Error: {e}")
        # save data_tuple to csv
        random_string = "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        )
        pd.DataFrame(batch).to_csv(
            rf"{APP_DIR}\failed_queries\data_tuple_{batch_num}_{random_string}.csv",
            index=False,
        )


def run_sf_task(task_name: str):
    try:
        sf_conn = get_sf_connection()
        cursor = sf_conn.cursor()
        cursor.execute(f"EXECUTE TASK {task_name}")
        sf_conn.close()
        print(f"✅ Successfully triggered task: {task_name}")
    except Exception as e:
        print(f"❌ Error triggering task {task_name}: {e}")


def pd_dataframe_2_snowflake_parallel(
    insert_query, data_tuple, batch_size=1000, max_workers=20
):
    batches = [
        (data_tuple[i : i + batch_size], i // batch_size + 1)
        for i in range(0, len(data_tuple), batch_size)
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for batch, batch_num in batches:
            futures.append(
                executor.submit(sf_insert_batch, insert_query, batch, batch_num)
            )
        concurrent.futures.wait(futures)


def run_sf_select_query(query: str):
    """
    Runs a SELECT query on Snowflake and returns the results as a list of tuples.
    """
    sf_conn = get_sf_connection()
    cursor = sf_conn.cursor()

    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return results

    finally:
        cursor.close()
        sf_conn.close()


def inserted_company_and_date(econ_id, metric_type="pd"):
    sql_query = (
        "SELECT company_id, date FROM cri_pd_prod_marcus."
        + metric_type
        + "_company_individual_daily_historical where econ_id = %s group by company_id, date"
        % (econ_id)
    )
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute(sql_query)

        results = {}

        for company_id, date in cursor.fetchall():
            formatted_date = date.strftime("%Y-%m-%d")
            if company_id not in results:
                results[company_id] = np.array([formatted_date])
            else:
                results[company_id] = np.append(results[company_id], formatted_date)
        # Close the cursor and database connection
        cursor.close()
        conn.close()

        return results

    except mysql.connector.Error as error:
        print(f"Failed to execute command: {error}")
        return {}  # Return an empty dictionary in case of an error


def execute_sql_query(sql_query, operation_type="select"):
    """
    - can run multiple sql in the same session. seperate query by ';'
    - if insert / alter table, set operation_type to change to commit the change.
    """
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()

        queries = sql_query.split(";")  # Split multiple queries
        for query in queries:
            query = query.strip()
            if query:
                cursor.execute(query)

        if operation_type.lower() == "insert" or operation_type.lower() == "change":
            conn.commit()
            return "Operation completed successfully."
        elif operation_type.lower() == "select":
            # Assuming only the last query returns results for select type
            columns = [col[0] for col in cursor.description]
            result = cursor.fetchall()
            return pd.DataFrame(result, columns=columns)
        else:
            return None

    except mysql.connector.Error as error:
        print(f"Failed to execute command: {error}")
        return None
    finally:
        cursor.close()
        conn.close()


def check_not_inserted(row, inserted_company_list):
    company_id = int(row["company_id"])
    date = row["date"]
    if company_id not in inserted_company_list:
        return True
    else:
        if date in inserted_company_list[company_id]:
            return False
        else:
            return True


def daily_data_preprocessing(mat_data, calibration_date, operation_time):
    df = pd.DataFrame()

    for data in mat_data:
        # print('reading', data)
        new_df = pd.DataFrame(data["pd60h"])
        df = pd.concat([df, new_df])

    # cast column names
    column_list = [
        "company_id",
        "yyyy","mm","dd", 
        "PD_1", "PD_2", "PD_3", "PD_4", "PD_5", "PD_6", "PD_7", "PD_8", "PD_9", "PD_10", 
        "PD_11", "PD_12", "PD_13", "PD_14", "PD_15", "PD_16", "PD_17", "PD_18", "PD_19", "PD_20", 
        "PD_21", "PD_22", "PD_23", "PD_24", "PD_25", "PD_26", "PD_27", "PD_28", "PD_29", "PD_30", 
        "PD_31", "PD_32", "PD_33", "PD_34", "PD_35", "PD_36", "PD_37", "PD_38", "PD_39", "PD_40", 
        "PD_41", "PD_42", "PD_43", "PD_44", "PD_45", "PD_46", "PD_47", "PD_48", "PD_49", "PD_50", 
        "PD_51", "PD_52", "PD_53", "PD_54", "PD_55", "PD_56", "PD_57", "PD_58", "PD_59", "PD_60",
    ]
    df.columns = column_list

    # filter out the line which does not have PD value
    df = df[~df[column_list[4:]].isna().any(axis=1)]

    # add column
    # df['econ_id'] = '-99'
    df["data_date"] = pd.to_datetime(
        df[["yyyy", "mm", "dd"]].astype(int).astype(str).agg("-".join, axis=1),
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    df["calibration_date"] = datetime.strptime(calibration_date, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )
    df["operation_time"] = operation_time
    df["company_id"] = df["company_id"].astype(int).astype(str)
    df = df.astype(str)

    print('finish data preprocesing')
    return df[
        [
            "data_date",
            "company_id",
            "calibration_date",
            "operation_time", 
            "PD_1", "PD_2", "PD_3", "PD_4", "PD_5", "PD_6", "PD_7", "PD_8", "PD_9", "PD_10", 
            "PD_11", "PD_12", "PD_13", "PD_14", "PD_15", "PD_16", "PD_17", "PD_18", "PD_19", "PD_20", 
            "PD_21", "PD_22", "PD_23", "PD_24", "PD_25", "PD_26", "PD_27", "PD_28", "PD_29", "PD_30", 
            "PD_31", "PD_32", "PD_33", "PD_34", "PD_35", "PD_36", "PD_37", "PD_38", "PD_39", "PD_40", 
            "PD_41", "PD_42", "PD_43", "PD_44", "PD_45", "PD_46", "PD_47", "PD_48", "PD_49", "PD_50", 
            "PD_51", "PD_52", "PD_53", "PD_54", "PD_55", "PD_56", "PD_57", "PD_58", "PD_59", "PD_60",
        ]
    ]


def revision_data_preprocessing(mat_data, calibration_date, operation_time):
    df = pd.DataFrame()

    for data in mat_data:
        # print('reading', data)
        new_df = pd.DataFrame(data["PdTotal"]).transpose()
        df = pd.concat([df, new_df])

    # cast column names
    column_list = [
        "company_id",
        "data_date",
        "PD_1","PD_2","PD_3","PD_4","PD_5","PD_6","PD_7","PD_8","PD_9","PD_10",
        "PD_11","PD_12","PD_13","PD_14","PD_15","PD_16","PD_17","PD_18","PD_19","PD_20",
        "PD_21","PD_22","PD_23","PD_24","PD_25","PD_26","PD_27","PD_28","PD_29","PD_30",
        "PD_31","PD_32","PD_33","PD_34","PD_35","PD_36","PD_37","PD_38","PD_39","PD_40",
        "PD_41","PD_42","PD_43","PD_44","PD_45","PD_46","PD_47","PD_48","PD_49","PD_50",
        "PD_51","PD_52","PD_53","PD_54","PD_55","PD_56","PD_57","PD_58","PD_59","PD_60",
    ]
    df.columns = column_list

    # filter out the line which does not have PD value
    df = df[~df[column_list[3:]].isna().any(axis=1)]

    # add column
    df["econ_id"] = "-99"
    df["calibration_date"] = calibration_date
    df["operation_time"] = operation_time
    df["data_date"] = pd.to_datetime(df["data_date"], format="%Y%m%d").dt.strftime(
        "%Y-%m-%d"
    )
    df.loc[:, "company_id"] = df["company_id"] / 1000
    df.loc[:, "company_id"] = df["company_id"].astype(int).astype(str)

    # print('finish data preprocesing')
    return df[
        [
            "data_date",
            "econ_id",
            "company_id",
            "calibration_date",
            "operation_time",
            "PD_1","PD_2","PD_3","PD_4","PD_5","PD_6","PD_7","PD_8","PD_9","PD_10",
            "PD_11","PD_12","PD_13","PD_14","PD_15","PD_16","PD_17","PD_18","PD_19","PD_20",
            "PD_21","PD_22","PD_23","PD_24","PD_25","PD_26","PD_27","PD_28","PD_29","PD_30",
            "PD_31","PD_32","PD_33","PD_34","PD_35","PD_36","PD_37","PD_38","PD_39","PD_40",
            "PD_41","PD_42","PD_43","PD_44","PD_45","PD_46","PD_47","PD_48","PD_49","PD_50",
            "PD_51","PD_52","PD_53","PD_54","PD_55","PD_56","PD_57","PD_58","PD_59","PD_60",
        ]
    ]


def xlsx_2_df(directory_path):
    # Use os.path.join to create the complete file path
    file_path = os.path.join(directory_path)

    # Use try-except block to handle potential errors when reading the Excel file
    try:
        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file_path)
        return df
        # Now you can work with the DataFrame 'df' as needed
        print(df.head())  # Display the first few rows of the DataFrame
    except Exception as e:
        print(f"Error: {e}")


def revision_operation_df(df):
    df = (
        df.groupby(["econ_id", "company_id", "calibration_date", "operation_time"])
        .agg(data_start_date=("data_date", "min"), data_end_date=("data_date", "max"))
        .reset_index()
    )
    df["operation_action"] = "revision"

    return df[
        [
            "data_start_date",
            "data_end_date",
            "econ_id",
            "company_id",
            "calibration_date",
            "operation_time",
            "operation_action",
        ]
    ]


def exclusion_preprocessing(df):
    for col in df.columns:
        if df[col].dtype == np.int64:
            df[col] = df[col].astype(str)

    df = df.fillna("-1")
    df["Stopped Excluding On (PD date)"].replace(
        "-1", "9999-12-31 00:00:00", inplace=True
    )
    df["Date the error was found"].replace("-1", "9999-12-31 00:00:00", inplace=True)
    return df[
        [
            "Region",
            "Company Number",
            "country",
            "Ticker",
            "Company Name",
            "Excluded Since (PD date)",
            "Stopped Excluding On (PD date)",
            "Date the error was found",
            "No.",
        ]
    ]


def as_daily_data_preprocessing(mat_data, calibration_date, operation_time):
    df = pd.DataFrame()

    for data in mat_data:
        # print('reading', data)
        new_df = pd.DataFrame(data["as"])
        df = pd.concat([df, new_df])

    # cast column names
    column_list = [
        "company_id",
        "yyyy",
        "mm",
        "dd",
        "cdsvalue1",
        "cdsvalue2",
        "cdsvalue3",
        "cdsvalue4",
        "cdsvalue5",
    ]
    df.columns = column_list

    # filter out the line which does not have PD value
    df = df[~df[column_list[4:]].isna().any(axis=1)]

    # add column
    df["data_date"] = pd.to_datetime(
        df[["yyyy", "mm", "dd"]].astype(int).astype(str).agg("-".join, axis=1),
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    df["calibration_date"] = datetime.strptime(calibration_date, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )
    df["operation_time"] = operation_time
    df["company_id"] = df["company_id"].astype(int).astype(str)

    df = df.astype(str)
    # print('finish data preprocesing')
    return df[
        [
            "data_date",
            "company_id",
            "calibration_date",
            "operation_time",
            "cdsvalue1",
            "cdsvalue2",
            "cdsvalue3",
            "cdsvalue4",
            "cdsvalue5",
        ]
    ]


def as_data_preprocessing(
    df,
    calibration_date="9999-12-31 00:00:00",
    operation_time="9999-12-31 00:00:00",
    econ_id="-99",
):
    # filter out the line which does not have PD value
    df = df[~df.isna()]

    # add column
    df["calibration_date"] = calibration_date
    df["operation_time"] = operation_time
    df["data_date"] = df["Date"]
    df["company_id"] = df["U3_ID"].astype(str)

    # float to str
    df[["Y1", "Y2", "Y3", "Y4", "Y5"]] = df[["Y1", "Y2", "Y3", "Y4", "Y5"]].astype(str)
    # fill na
    df = df.fillna("-99")

    # print('finish data preprocesing')
    return df[
        [
            "data_date",
            "company_id",
            "calibration_date",
            "operation_time",
            "Y1",
            "Y2",
            "Y3",
            "Y4",
            "Y5",
        ]
    ]


def pdir_daily_data_preprocessing(
    mat_data, calibration_date, operation_time, transpose=False
):
    df = pd.DataFrame()

    for data in mat_data:
        try:
            new_df = pd.DataFrame(data["pdirnum"])
        except:
            new_df = pd.DataFrame(data["pdirNum"])

        if transpose:
            new_df = new_df.transpose()
        df = pd.concat([df, new_df])

    # cast column names
    column_list = ["company_id", "yyyy", "mm", "dd", "pdir"]
    df.columns = column_list

    # filter out the line which does not have PD value
    df = df[~df["pdir"].isna()]

    # add column
    df["econ_id"] = "-99"
    df["data_date"] = pd.to_datetime(
        df[["yyyy", "mm", "dd"]].astype(int).astype(str).agg("-".join, axis=1),
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    # df["calibration_date"] = datetime.strptime(calibration_date, "%Y%m%d").strftime(
    #     "%Y-%m-%d"
    # )
    # df.loc[:, "operation_time"] = operation_time
    df.loc[:, "company_id"] = df["company_id"].astype(int).astype(str)
    df.loc[:, "pdir"] = df["pdir"].astype(str)

    # print('finish data preprocesing')
    return df[
        [
            "data_date",
            "econ_id",
            "company_id",
            # "calibration_date",
            # "operation_time",
            "pdir",
        ]
    ]


def pdir_revision_mat_2_pd(mat_files, pdir=1):
    df = pd.DataFrame()
    regex_pattern = r"resultRating_(\d+).mat"

    for mat_file in mat_files:
        company_id = re.search(r"(\d+)", mat_file).group(1)
        data = h5py.File(mat_file, "r")["resultRating"]
        if len(data.shape) == 1:
            continue
        new_df = pd.DataFrame(data).transpose()
        new_df.fillna("-99")

        # retrieve columns name (company id)
        new_df.columns = np.array(
            h5py.File(os.path.join("tmp", "resultFirms_%s.mat" % (company_id)), "r")[
                "resultFirms"
            ]
        )

        # retrieve row name (date)
        row_name = np.array(
            h5py.File(os.path.join("tmp", "resultDates_%s.mat" % (company_id)), "r")[
                "fullPeriod"
            ]
        )
        if pdir == 1:
            row_name = row_name[0]
        new_df.set_index(row_name, inplace=True)

        # melt the df from 2d to 1d
        new_df = pd.melt(
            new_df.reset_index(),
            id_vars=["index"],
            var_name="company_id",
            value_name="pdir",
        )
        new_df.columns = ["data_date", "company_id", "pdir"]

        df = pd.concat([df, new_df])

    return df


def pdir_revision_data_preprocessing(
    df,
    calibration_date="9999-12-31 00:00:00",
    operation_time="9999-12-31 00:00:00",
    econ_id="-99",
):
    # filter out the line which does not have value
    df = df[~df["pdir"].isna()]

    # add column
    df.loc[:, "econ_id"] = econ_id
    df.loc[:, "calibration_date"] = calibration_date
    df.loc[:, "operation_time"] = operation_time
    df.loc[:, "pdir"] = df["pdir"].astype(str)
    df.loc[:, "company_id"] = df["company_id"].astype(str)
    df.loc[:, "data_date"] = df["data_date"].astype(str)

    # fill na
    df = df.fillna("-99")

    # print('finish data preprocesing')
    return df[
        [
            "data_date",
            "econ_id",
            "company_id",
            "calibration_date",
            "operation_time",
            "pdir",
        ]
    ]


def pdir_historical_mat_2_pd(mat_files):
    df = pd.DataFrame()
    regex_pattern = r"resultRating_(\d+).mat"

    for mat_file in mat_files:
        parent_dir = os.path.dirname(mat_file)
        econ_id = mat_file.split("_")[-1].replace(".mat", "")
        data = h5py.File(mat_file, "r")["resultRating"]
        if len(data.shape) == 1:
            continue
        new_df = pd.DataFrame(data).transpose()
        # retrieve columns name (company id)
        new_df.columns = np.array(
            h5py.File(os.path.join(parent_dir, f"resultFirms_{int(econ_id)}.mat"), "r")[
                "resultFirms"
            ]
        )

        # retrieve row name (date)
        row_name = np.array(
            h5py.File(os.path.join(parent_dir, f"resultDates_{int(econ_id)}.mat"), "r")[
                "fullPeriod"
            ]
        )
        print(f"Row Names (Dates) Shape: {row_name.shape}")
        if row_name.ndim > 1:
            row_name = row_name.flatten()
            print(f"Flattened Row Names Shape: {row_name.shape}")
        new_df.set_index(row_name, inplace=True)

        # melt the df from 2d to 1d
        new_df = pd.melt(
            new_df.reset_index(),
            id_vars=["index"],
            var_name="company_id",
            value_name="pdir",
        )
        new_df.columns = ["date", "company_id", "pdir"]

        new_df["econ_id"] = econ_id

        df = pd.concat([df, new_df])

    return df


def pdir_historical_data_preprocessing(df, operation_time, yyyymm):
    # filter out the line which does not have value
    df = df[~df["pdir"].isna()].copy()

    # add column
    df.loc[:, "pdir"] = df["pdir"].astype(str)
    df.loc[:, "date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime(
        "%Y-%m-%d"
    )
    df.loc[:, "company_id"] = df["company_id"] / 1000
    df.loc[:, "company_id"] = df["company_id"].astype(int).astype(str)
    # df.loc[:, "operation_time"] = operation_time
    # df.loc[:, "calibration_date"] = (
    #     datetime.strptime(yyyymm, "%Y%m").replace(day=30).strftime("%Y-%m-%d")
    # )

    # fill na
    df = df.fillna("-99")

    # print('finish data preprocesing')
    return df[
        [
            "date",
            "econ_id",
            "company_id",
            # "calibration_date",
            # "operation_time",
            "pdir",
        ]
    ]


def execute_pdir_forward_fill_query(input_date: datetime):
    try:
        # Calculate the date minus 10 days
        calculated_date = input_date - timedelta(days=10)
        # Query 1: Create Temporary Table
        create_temp_table_query = f"""
        CREATE OR REPLACE TEMPORARY TABLE RankedData AS 
        WITH Ranked AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY company_id, data_date
                    ORDER BY operation_time DESC
                ) AS row_num
            FROM 
                CRI.CRI_PROD_MARCUS.pd_company_individual_daily_complete_latest
            WHERE
                data_date >= '{calculated_date.date()}'
        )
        SELECT
            company_id,
            data_date,
            pd_12,
            COALESCE(
                NULLIF(pdir, 0), 
                LAST_VALUE(NULLIF(pdir, 0)) IGNORE NULLS 
                OVER (
                    PARTITION BY company_id
                    ORDER BY data_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
            ) AS pdir_filled
        FROM Ranked
        WHERE row_num = 1;
        """

        # Query 2: Update Main Table using the Temporary Table
        update_query = f"""
        UPDATE CRI.CRI_PROD_MARCUS.pd_company_individual_daily_complete_latest AS main
        SET main.pdir = RankedData.pdir_filled
        FROM RankedData
        WHERE 
            main.company_id = RankedData.company_id 
            AND main.data_date = RankedData.data_date
            AND (main.pdir IS NULL OR main.pdir = 0)
            AND main.data_date >= '{calculated_date.date()}';
        """

        # Query 3: Drop the Temporary Table
        drop_temp_table_query = "DROP TABLE IF EXISTS RankedData;"

        # Establishing Snowflake connection
        sf_conn = get_sf_connection()
        cursor = sf_conn.cursor()

        # Executing the three queries
        cursor.execute(create_temp_table_query)
        print("✅ Temporary table created successfully.")

        cursor.execute(update_query)
        print("✅ Forward fill update executed successfully.")

        cursor.execute(drop_temp_table_query)
        print("✅ Temporary table dropped successfully.")

        # Closing the connection
        sf_conn.close()
        print(
            f"✅ Successfully completed forward fill task for date: {calculated_date.date()}"
        )
    except Exception as e:
        print(f"❌ Error executing forward fill task: {e}")


def get_calibration_date(target_date):
    # find the file
    # print("get_calibration_date: 1")
    file_list = get_file_list(
        snowflake_config_obj,
        os.path.join("/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily"),
    )
    # print("get_calibration_date: 2")
    cal_file = [
        item
        for item in file_list
        if item[:8] == target_date.strftime("%Y%m%d") and item[:1] <= "9"
    ]  # filter recent week data and filter out file that is not related.

    # print("get_calibration_date: 3")
    if len(cal_file) == 0:
        raise ValueError(
            "no cal file for your target date: %s, no data inserted."
            % (target_date.strftime("%Y%m%d"))
        )
    # print("get_calibration_date: 4")
    if len(cal_file) > 1:
        raise ValueError(
            "more than one cal file for your target date: %s, no data inserted."
            % (target_date.strftime("%Y%m%d"))
        )
    # print("get_calibration_date: 5")

    cal_file = cal_file[0]
    return cal_file, str(cal_file[-8:])


