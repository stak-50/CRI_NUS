import pandas as pd
import numpy as np
from src.helper import *
from datetime import datetime, timedelta, timezone
import scipy.io
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from functools import partial
from src.utils import save_data_as_parquet
from src.helper import pdir_historical_data_preprocessing


def process_pdir_batch(file_batch, operation_time, yyyymm, econ_id):
    mat_data = pdir_historical_mat_2_pd(file_batch)
    if len(mat_data) == 0:
        return

    try:
        df = pdir_historical_data_preprocessing(mat_data, operation_time, yyyymm)
        random_string = "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        )
        # Get current time in milliseconds
        timestamp_ms = int(time.time() * 1000)
        res = save_data_as_parquet(
            df,
            rf"{APP_DIR}\parquet_pdir_data\{timestamp_ms}_{random_string}_pdir_{yyyymm}_{econ_id}",
        )
        print("Data Saved!!")
        if not res:
            print(f"Error saving data as parquet")
            # save data as csv
            df.to_csv(
                rf"{APP_DIR}\failed_pdir_data\{timestamp_ms}_{random_string}_pdir_{yyyymm}_{econ_id}.csv",
                index=False,
            )
    except Exception as e:
        print(f"Error Processing Batch {e}")
    finally:
        for file in mat_data:
            file.close()  # Close the h5py file handles


def run_parallel_pdir_batches(
    files_batches, operation_time, yyyymm, econ_id, max_workers=4
):
    total_batches = len(files_batches)
    completed_batches = 0
    lock = Lock()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        process_fn = partial(
            process_pdir_batch,
            operation_time=operation_time,
            yyyymm=yyyymm,
            econ_id=econ_id,
        )

        futures = [executor.submit(process_fn, batch) for batch in files_batches]

        for _ in as_completed(futures):
            with lock:
                completed_batches += 1
                progress = (completed_batches / total_batches) * 100
                print(f"econ id: {econ_id}, progress: {progress:.2f}%")


def pdir_yearly_historical(yyyymm, start_econ_id=1, end_econ_id=200, file_amt_limit=50):
    """
    insert the 1988 - (current_year - 1) data into pdir_company_individual_daily_historical
    """

    create_tmp_folder("parquet_pdir_data")
    create_tmp_folder("failed_pdir_data")

    remote_file_paths = [
        os.path.join(
            "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Historical/"
            + yyyymm
            + "/Daily/Products/P5_Pdir"
            + f"/resultRating_{s}.mat"
        )
        for s in range(start_econ_id, end_econ_id + 1)
    ]

    operation_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"----- Operation Time: {operation_time} -----")
    print(f"----- Remote File Paths: {remote_file_paths} -----")

    if remote_file_paths:
        # filter out not expected file
        file_list = [
            os.path.join(
                smb_config_obj["shared_folder"],
                item,
            )
            for item in remote_file_paths
        ]

        files_batches = [
            file_list[i : i + file_amt_limit]
            for i in range(0, len(file_list), file_amt_limit)
        ]

        run_parallel_pdir_batches(files_batches, operation_time, yyyymm, "pdir", 5)
    print("finished inserting")


def get_pdir_data(yyyymm, econ_id):
    pdir_sub_path = os.path.join(
        "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Historical/"
        + yyyymm
        + "/Daily/Products/P5_Pdir"
        + f"/resultRating_{econ_id}.mat"
    )
    pdir_file = os.path.join(
        smb_config_obj["shared_folder"],
        pdir_sub_path,
    )
    mat_data = pdir_historical_mat_2_pd([pdir_file])
    return mat_data


def pdir_yearly_historical_partial(yyyymm, pd_df, mat_data, operation_time):
    df_date_company_id = pd_df[["data_date", "company_id"]]
    pdir_df = pdir_historical_data_preprocessing(mat_data, operation_time, yyyymm)
    # rename column date to data_date
    pdir_df = pdir_df.rename(columns={"date": "data_date"})

    # filter df by date & company_id in df_date_company_id
    # inner merge df (date, company_id) & df_date_company_id (data_date, company_id)
    pdir_df = pdir_df.merge(
        df_date_company_id,
        on=["data_date", "company_id"],
        how="inner",
    )
    pdir_df = pdir_df.rename(columns={"pdir": "pdir_extracted"})
    pdir_df[["data_date", "company_id", "pdir_extracted"]]
    print(pdir_df.head())

    # print number of rows in df & pdir_df before merge
    print("*=" * 50)
    print(f"(BEFORE MERGE) Number of rows in df: {len(pd_df)}")
    print(f"(BEFORE MERGE) Number of rows in pdir_df: {len(pdir_df)}")

    # merge df & pdir_df (left join)
    df = pd_df.merge(pdir_df, on=["data_date", "company_id"], how="left")
    # pdir
    df["PDIR"] = df["pdir_extracted"]
    df = df.drop(columns=["pdir_extracted"])

    # print number of rows in df & pdir_df after merge
    print(f"(AFTER MERGE) Number of rows in df: {len(df)}")
    print(f"(AFTER MERGE) Diff: {len(df) - len(pdir_df)}")
    print("*=" * 50)
    return df


def pdir_daily(
    date=None, df_date_company_id=None, duplication_check=True, insert_daily_table=True
):
    if (
        date == None
    ):  # if it is a daily automate task, then read t-2 data. else read data of the target date.
        date = datetime.today() - timedelta(days=2)

    operation_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # find the file
    file_list = get_file_list(
        smb_config_obj,
        os.path.join("/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily"),
    )
    cal_file = [
        item
        for item in file_list
        if item[:8] == date.strftime("%Y%m%d") and item[:1] <= "9"
    ]  # filter recent week data and filter out file that is not related.

    if len(cal_file) == 0:
        return "no cal file for your target date: %s, no data inserted." % (
            date.strftime("%Y%m%d")
        )
    if len(cal_file) > 1:
        return "more than one cal file for your target date: %s, no data inserted." % (
            date.strftime("%Y%m%d")
        )

    cal_file = cal_file[0]
    data_date = str(cal_file[:8])
    calibration_date = str(cal_file[-8:])

    remote_path = os.path.join(
        "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily/"
        + cal_file
        + "/Products/P5_Pdir"
    )
    sub_file_list = get_file_list(smb_config_obj, remote_path)

    # download the file
    # connect_and_download_file(
    #     smb_config_obj, sub_file_list, local_file_path, remote_path, 0
    # )
    # ---------------------------------------------------------------------------------------------------------------------------------------
    mat_data = []
    mat_files = [
        os.path.join(
            smb_config_obj["shared_folder"] +
            remote_path + "/" +
            item
        )
        for item in sub_file_list
        if item.startswith("pdirNum_")
    ]

    econs_covered = set()
    for mat_file in mat_files:
        try:
            data = loadmat(mat_file)
            mat_data.append(data)

            econ_id = mat_file.split("_")[-1].split(".")[0]
            econs_covered.add(econ_id)
        except Exception as e:
            print(f"Error loading the file '{mat_file}': {e}")

    if len(mat_data) == 0:
        return "no data found"

    df = pdir_daily_data_preprocessing(mat_data, calibration_date, operation_time)
    # rename column date to data_date
    df = df.rename(columns={"date": "data_date"})

    # cast all column to str
    df = df.astype(str)
    df_date_company_id = df_date_company_id.astype(str)

    # filter df by date & company_id in df_date_company_id
    # inner merge df (date, company_id) & df_date_company_id (data_date, company_id)
    df = df.merge(
        df_date_company_id,
        on=["data_date", "company_id"],
        how="inner",
    )
    df = df.rename(columns={"pdir": "pdir_extracted"})
    return (df[["data_date", "company_id", "pdir_extracted"]], econs_covered)
