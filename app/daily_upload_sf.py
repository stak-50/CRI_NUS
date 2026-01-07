from src.modules import *

import os
from datetime import datetime
import time
import schedule
import sys
import argparse
from src.helper import get_calibration_date, pd_dataframe_2_snowflake_parallel, data_preprocessing
from datetime import datetime, timedelta
import os
import logging

# List of econ to check the file for
def _get_econ_list():
    """Return the list of economic indicators."""
    return sorted(
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 17, 18, 19, 20, 22, 15, 16, 26, 
         75, 84, 23, 24, 25, 31, 33, 34, 35, 36, 37, 38, 40, 43, 45, 46, 47, 48, 
         49, 51, 55, 56, 58, 61, 64, 65, 66, 67, 70, 74, 76, 77, 78, 79, 81, 82, 
         88, 89, 29, 30, 32, 42, 52, 54, 69, 72, 85, 59, 27, 39, 50, 57, 63, 
         83, 86, 71, 92, 95, 96, 97, 100, 102, 103, 107,
         # 60, 163, 
        ]
    )

# Get product file list
def _get_files_list(
    data_date: datetime,
    calibration_date: str,
    product_type: str,
    product_subfolder: str,
):
    remote_path = os.path.join(
        "/CRI3/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily/",
        data_date.strftime("%Y%m%d") + "_cali_" + calibration_date,
        "Products",
        product_subfolder,
    )
    file_list = [f"{product_type}_{i_econ}.mat" for i_econ in _get_econ_list()]

    file_paths = [os.path.join(remote_path, item) for item in file_list]
    return file_paths


# Check if there is missing file 
def check_files(
    data_date: datetime,
    calibration_date: str,
    product_type: str,
    product_subfolder: str,
):
    file_paths = _get_files_list(
        data_date, calibration_date, product_type, product_subfolder
    )
    # all_exists = self._check_files_exist(file_paths)
    all_exists = all(os.path.isfile(f) for f in file_paths)
    if not all_exists:
        print("ERROR: Some Files Doesn't Exists")
        missing_files = [f for f in file_paths if not os.path.isfile(f)]
        print(f"missing_files: {missing_files}")
        raise Exception("Error: Some Files Doesn't Exists")

    return True

# Check if there is missing file, stop at 11PM
def run_check_files(data_date: datetime, calibration_date: str):
    current_hour = datetime.now().hour
    if current_hour == 23:  # 11 PM
        print("It's 11 PM, exiting...")
        return False

    try:
        check_files(data_date, calibration_date, "pd60h", "P2_Pd")
        return True
    except Exception as e:
        print(f"Error occurred: {e}")
        print("Retrying in 30 minutes...")
        time.sleep(900)  # Sleep for 30 minutes
        return run_check_files(data_date, calibration_date)  # Retry recursively

# get the latest weekday (mon-fri)
def get_latest_trading_date(target_date):
    if target_date.weekday() == 5:  # Saturday
        return target_date - timedelta(days=1)
    elif target_date.weekday() == 6:  # Sunday
        return target_date - timedelta(days=2)
    return target_date


# Man function for daily upload
def run_daily_upload_sf(start=None, end=None):
    #main loop
    time_delta=1
    while True:
        try:
            # Set start date if not given
            # always set to yester day if run automatically via scheduler
            if start is None:
                start = get_latest_trading_date(datetime.today() - timedelta(days=time_delta))
            if end is None:
                end = get_latest_trading_date(datetime.today() - timedelta(days=time_delta))
            print(f"start: {start} end: {end}")
            
            # Get calibration date
            (cal_files, calibration_date) = get_calibration_date(end)
            
            # check if all file is there and ready for upload
            print(
                "run_check_files(end, calibration_date)",
                run_check_files(end, calibration_date),
            )
            if not run_check_files(end, calibration_date):
                raise ValueError("Some files not found")

            # Call your pipeline function
            print("=" * 20)
            print("Uploading Excluded Companies.......")
            print("=" * 20)
            upload_excluded_companies_docker(start, True)

            print("=" * 20)
            print("Uploading Credit Events Raw.......")
            print("=" * 20)
            upload_credit_events_raw_docker(start, end, True)

            print("=" * 20)
            print("Uploading PD Daily Data Raw.......")
            print("=" * 20)
            get_pd_daily_sf_by_range(start, end, True)


            # Update table to signal for client data generator
            print("=" * 20)
            print("Update SNOWFLAKEHISTORY TABLE.......")
            print("=" * 20)
            insert_query = "INSERT INTO CRI.CRI_PROD_MARCUS.SNOWFLAKE_UPDATE_HISTORY (date_int, source, operation_date) "
            insert_query += "VALUES (" + ", ".join(["%s" for _ in range(3)]) + ") "
            data_tuple=[(cal_files[:8], cal_files, datetime.now())]
            print("data_tuple_insert >>>", len(data_tuple))
            pd_dataframe_2_snowflake_parallel(insert_query, data_tuple, 1000)
            return

        except ValueError as e:
            # looping if file not found instead of quiting
            print("ValueError: ", e)
            print("trying again")
            time.sleep(15*60)
            continue
        except Exception as e:
            print("Exception: ", e)
            return


# run the script in automatically every day at 12:30 PM
# if __name__ == "__main__":
#     # start the cron job that runs tuesday to saturday at 12:30 PM
#     # Day after weekday, working on yesterday data

#     os.environ['TZ'] = 'Asia/Singapore'
#     #time.tzset()
#     schedule.every().tuesday.at("16:30").do(run_daily_upload_sf)
#     schedule.every().wednesday.at("16:30").do(run_daily_upload_sf)
#     schedule.every().thursday.at("16:30").do(run_daily_upload_sf)
#     schedule.every().friday.at("16:30").do(run_daily_upload_sf)
#     schedule.every().saturday.at("16:30").do(run_daily_upload_sf)

#     # Loop till scheduled time
#     while True:
#         now = datetime.now()
#         print(now)
#         try:
#             schedule.run_pending()
#         except Exception as e:
#             print(f"[{datetime.now()}] Error in scheduler: {e}")
#         finally:
#             time.sleep(60)

# # run the script manually
# if __name__ == "__main__":
#     # year=2025
#     # month=9
#     # date=29
#     start = datetime(2025, 1, 1)
#     end = datetime(2026, 1, 1)
#     run_daily_upload_sf()
#     print(f"Done {year} {month} {date}")

def run_pd_daily_upload():
    # 1) Get cleaned DataFrame
    df = data_preprocessing()

    # 2) Convert df to tuples in correct order
    data_tuple = list(df.itertuples(index=False, name=None))

    # 3) Read insert settings from config
    insert_settings = config["insert_settings"]
    batch_size = insert_settings["batch_size"]
    max_workers = insert_settings["max_workers"]

    # 4) Build INSERT SQL (match your real table/columns)
    insert_query = """
        INSERT INTO CRI_TEST.PD_DAILY.PD_DAILY_TEST (
            comp_id, date,
            pd_1, pd_3, pd_6,
            pd_12, pd_24, pd_36, pd_48, pd_60
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # 5) Call batch insert helper
    pd_dataframe_2_snowflake_parallel(
        insert_query=insert_query,
        data_tuple=data_tuple,
        batch_size=batch_size,
        max_workers=max_workers,
    )

if __name__ == "__main__":
    # SIMPLE TEST: .mat -> DataFrame -> CRI_TEST.PD_DAILY.PD_DAILY_TEST
    # df = data_preprocessing()
    setup_logging()
    logging.info("Starting PD upload...")
    try:
        run_pd_daily_upload()
        logging.info("PD upload succeeded")
    except Exception:
        logging.exception("PD upload failed")
    raise
    # print("Rows in df:", len(df))
    # print("DEBUG: starting test insert")
    # print("DEBUG: df shape:", df.shape)
    # print("DEBUG: df head:\n", df.head())
    # # NEW: normalize types so Snowflake can bind them
    # df["comp_id"] = df["comp_id"].astype(int)
    # df["date"] = pd.to_datetime(df["date"]).dt.date          # pure Python date objects
    # for col in ["pd_1", "pd_3", "pd_6", "pd_12", "pd_24", "pd_36", "pd_48", "pd_60"]:
    #     df[col] = df[col].astype(float)


    # data_tuple = list(df.itertuples(index=False, name=None))

    # insert_query = """
    #     INSERT INTO CRI_TEST.PD_DAILY.PD_DAILY_TEST (
    #         comp_id,
    #         date,
    #         pd_1,
    #         pd_3,
    #         pd_6,
    #         pd_12,
    #         pd_24,
    #         pd_36,
    #         pd_48,
    #         pd_60
    #     )
    #     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    # """
    # print("DEBUG: about to insert rows:", len(data_tuple))
    # print("DEBUG: sample tuple:", data_tuple if data_tuple else None)

    # pd_dataframe_2_snowflake_parallel(
    #     insert_query=insert_query,
    #     data_tuple=data_tuple,
    #     batch_size=1000,
    #     max_workers=20,
    # )
    


    # print("Done inserting into CRI_TEST.PD_DAILY.PD_DAILY_TEST")



