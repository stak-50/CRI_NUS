import pandas as pd
import numpy as np
from .helper import *
from datetime import datetime, timedelta, timezone
import scipy.io
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from functools import partial
from .utils import *
from .pdir_modules import pdir_daily, pdir_yearly_historical_partial, get_pdir_data


def process_pd_batch(file_batch, pdir_mat_data, operation_time, yyyymm, econ_id):
    mat_data = []

    for mat_file in file_batch:
        mat_data.append(h5py.File(mat_file, "r"))

    try:
        df = data_preprocessing(mat_data, econ_id, operation_time, yyyymm)
        # PDIR
        updated_df = pdir_yearly_historical_partial(
            yyyymm, df, pdir_mat_data, operation_time
        )
        random_string = "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        )
        # Get current time in milliseconds
        timestamp_ms = int(time.time() * 1000)
        res = save_data_as_parquet(
            updated_df,
            rf"{APP_DIR}\parquet_data\{timestamp_ms}_{random_string}_pd_{yyyymm}_{econ_id}",
        )
        print("Data Saved!!")
        if not res:
            print(f"Error saving data as parquet")
            # save data as csv
            updated_df.to_csv(
                rf"{APP_DIR}\failed_data\{timestamp_ms}_{random_string}_pd_{yyyymm}_{econ_id}.csv",
                index=False,
            )
    except Exception as e:
        print(f"Error Processing Batch {e}")
    finally:
        for file in mat_data:
            file.close()  # Close the h5py file handles


def extract_company_ids(file_batch):
    return [int(file.split("_")[-1].replace(".mat", "")) for file in file_batch]


def run_parallel_pd_batches(
    files_batches, operation_time, yyyymm, econ_id, max_workers=4
):
    total_batches = len(files_batches)
    completed_batches = 0
    lock = Lock()
    pdir_mat_data = get_pdir_data(yyyymm, econ_id)

    def filter_pdir_mat_data(company_ids):
        return pdir_mat_data[pdir_mat_data["company_id"].isin(company_ids)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        process_fn = partial(
            process_pd_batch,
            operation_time=operation_time,
            yyyymm=yyyymm,
            econ_id=econ_id,
        )

        futures = [
            executor.submit(
                process_fn, batch, filter_pdir_mat_data(extract_company_ids(batch))
            )
            for batch in files_batches
        ]
        for _ in as_completed(futures):
            with lock:
                completed_batches += 1
                progress = (completed_batches / total_batches) * 100
                print(f"econ id: {econ_id}, progress: {progress:.2f}%")


def pd_yearly_historical(yyyymm, start_econ_id=1, end_econ_id=200, file_amt_limit=50):
    """
    insert the 1988 - (current_year - 1) data into pd_company_individual_daily_historical
    """

    create_tmp_folder("parquet_data")
    create_tmp_folder("failed_data")

    # insert_query = (
    #     "INSERT INTO pd_company_individual_daily_complete (data_date, company_id, calibration_date, operation_time, PDIR, "
    #     + ", ".join(["PD_" + str(i) for i in range(1, 61)])
    #     + ") "
    # )
    # insert_query += "VALUES (" + ", ".join(["%s" for _ in range(1, 66)]) + ") "

    finish_econ_id_list = []
    remote_paths = [
        os.path.join(
            "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Historical/"
            + yyyymm
            + "/Daily/Products/P2_Pd/"
            + f"{s}/individualPD/"
        )
        for s in range(start_econ_id, end_econ_id + 1)
    ]

    operation_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"----- Operation Time: {operation_time} -----")
    print(f"----- Remote Paths: {remote_paths} -----")

    for remote_path in remote_paths:
        # get econ_id from remote_path
        econ_id = os.path.basename(os.path.split(os.path.split(remote_path)[0])[0])

        if econ_id not in finish_econ_id_list:

            print("start process econ id: ", econ_id)

            while True:
                # get the list of file
                file_list = get_file_list(smb_config_obj, remote_path)
                print(f"----- File List: {len(file_list)} -----")

                if not file_list:
                    break

                # filter out not expected file
                file_list = [
                    os.path.join(
                        smb_config_obj["shared_folder"],
                        remote_path,
                        item,
                    )
                    for item in file_list
                    if item.startswith("pd_")
                ]
                initial_file_list_len = len(file_list)
                # print(file_list[:5])
                # file_list = file_list[:5]
                print(f"Econ Id: {econ_id} num of .mat files: {initial_file_list_len}")

                files_batches = [
                    file_list[i : i + file_amt_limit]
                    for i in range(0, len(file_list), file_amt_limit)
                ]

                run_parallel_pd_batches(
                    files_batches, operation_time, yyyymm, econ_id, 5
                )

                print("finish inserting", remote_path)
                finish_econ_id_list.append(econ_id)
                print("finished econ id:", finish_econ_id_list)
                break
        else:
            print("already inserted econ : %s, will not process again" % (econ_id))


def get_pd_daily_sf_by_range(
    start_date: datetime, end_date: datetime, direct_insert: bool = False
) -> pd.DataFrame:
    try:
        create_tmp_folder("parquet_data_daily")

        all_dfs = []
        current_date = start_date

        while current_date <= end_date:
            if current_date.weekday() < 5:  # 0 to 4 = Mon to Fri
                daily_df = pd_daily_sf(
                    current_date.strftime("%Y-%m-%d")
                )  # Your real fetch function
                if isinstance(daily_df, str):
                    print(daily_df)
                elif not daily_df.empty:
                    all_dfs.append(daily_df)
            current_date += timedelta(days=1)

        merged_df = pd.concat(all_dfs, ignore_index=True)
        merged_df = merged_df.sort_values(by="current_trading_date", ascending=True)
        merged_df = merged_df.drop_duplicates(
            subset=["company_id", "data_date"], keep="last"
        )

        # pdir
        merged_df["date"] = pd.to_datetime(merged_df["data_date"], format="%Y-%m-%d")
        # merged_df["PDIR"] = calculate_pdir(merged_df)

        # filtered_df = merged_df[
        #     (merged_df["date"] >= start_date) & (merged_df["date"] <= end_date)
        # ]
        filtered_df = merged_df.copy()

        print(f"Total rows: {len(filtered_df)}")
        print(filtered_df.head())
        print(filtered_df.isnull().values.any())
        filtered_df = filtered_df.drop(columns=["current_trading_date", "date"])
        ordered_columns = [
            "data_date",
            "company_id",
            "calibration_date",
            "operation_time",
            "PDIR",
        ] + [f"PD_{i}" for i in range(1, 61)]

        # Reorder the DataFrame
        filtered_df = filtered_df[ordered_columns]

        random_string = "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        )
        # Get current time in milliseconds
        timestamp_ms = int(time.time() * 1000)
        res = save_data_as_parquet(
            filtered_df,
            rf"{APP_DIR}\parquet_data_daily\{timestamp_ms}_{random_string}_pd_daily",
        )
        print("Data Saved!!")
        if not res:
            print(f"Error saving data as parquet")
            # save data as csv
            filtered_df.to_csv(
                rf"{APP_DIR}\failed_data\{timestamp_ms}_{random_string}_pd_daily.csv",
                index=False,
            )
        if direct_insert:
            insert_query = (
                "INSERT INTO CRI.CRI_PROD_MARCUS.pd_company_individual_daily_complete_latest (data_date, company_id, calibration_date, operation_time, PDIR, "
                + ", ".join(["PD_" + str(i) for i in range(1, 61)])
                + ") "
            )
            insert_query += "VALUES (" + ", ".join(["%s" for _ in range(1, 66)]) + ") "

            # Fill NaN values in PDIR column with 0
            filtered_df["PDIR"] = filtered_df["PDIR"].fillna(0)

            # insert data to mysql
            data_tuple = tuple(
                tuple(record) for record in filtered_df.to_records(index=False)
            )
            execute_pdir_forward_fill_query(start_date)
            pd_dataframe_2_snowflake_parallel(insert_query, data_tuple, 1000)
            run_sf_task("CRI.CRI_PROD_MARCUS.refresh_pd_downstreams_daily")

    except Exception as e:
        print(f"Error Processing Batch {e}")


def clean_record(row):
    cleaned = []
    for val in row:
        # Handle NaNs
        if isinstance(val, float) and math.isnan(val):
            cleaned.append(None)
        elif isinstance(val, (np.float32, np.float64)) and np.isnan(val):
            cleaned.append(None)
        # Convert numpy integers/floats
        elif isinstance(val, (np.integer, np.floating)):
            cleaned.append(val.item())
        # Convert numpy.datetime64 or pandas.Timestamp to Python datetime
        elif isinstance(val, (np.datetime64, pd.Timestamp)):
            val = pd.to_datetime(val).to_pydatetime()
            cleaned.append(val)
        # Convert datetime.datetime to string if needed
        elif isinstance(val, datetime):
            cleaned.append(val.strftime("%Y-%m-%d"))  # or keep as datetime if supported
        else:
            cleaned.append(val)
    return tuple(cleaned)


def load_historical_credit_events_raw_docker(direct_insert: bool = False):
    # find the file
    file_list = get_file_list(
        smb_config_obj,
        base_path := os.path.join(
            "OfficialTest_AggDTD_SBChinaNA",
            "ProductionData",
            "ModelCalibration",
            "202504",
            "IDMTData",
            "SmartData",
            "FirmHistory",
            "Before_MA",
        ),
    )

    full_base_path = os.path.join(
        smb_config_obj["shared_folder"], base_path.lstrip("/")
    )

    historical_merged_df = pd.DataFrame()
    pattern = re.compile(r"^CreditEvent_\d+\.mat$")
    for item in file_list:
        if not pattern.match(item):
            continue
        full_path = os.path.join(full_base_path, item)
        res_df = load_mat_as_dataframe_old(full_path, "creditEventCRI")
        res_df.columns = [
            "CompanyID",
            "DateStr",
            "EventID",
            "DefaultType",
            "DateStr2",
            "EventID2",
        ]
        res_df["DefaultDate"] = pd.to_datetime(res_df["DateStr"], format="%Y%m%d")
        res_df = res_df.drop(columns=["DateStr"])
        res_df = res_df.where(pd.notnull(res_df), None)
        res_df = res_df.applymap(clean_cell)

        res_df = res_df[["DefaultDate", "CompanyID", "DefaultType", "EventID"]]
        res_df["AdditionalComments"] = None
        historical_merged_df = pd.concat([historical_merged_df, res_df])
    historical_merged_df = historical_merged_df.sort_values(by="DefaultDate")
    clean_df = historical_merged_df.where(pd.notnull(historical_merged_df), None)

    if direct_insert:
        # delete all data
        delete_query = "DELETE FROM CRI.CRI_PROD_MARCUS.credit_events_raw"
        pd_dataframe_2_snowflake_parallel(delete_query, ((),))

        insert_query = "INSERT INTO CRI.CRI_PROD_MARCUS.credit_events_raw (default_date, company_id, default_type, event_id, additional_comments) "
        insert_query += "VALUES (" + ", ".join(["%s" for _ in range(1, 6)]) + ") "
        # insert data to mysql
        df_obj = clean_df.astype(object)
        data_tuple = [clean_record(row) for row in df_obj.to_records(index=False)]
        for row in data_tuple:
            assert all(val == val or val is None for val in row)  # NaN fails val == val
        print("data_tuple_insert >>>", len(data_tuple))
        pd_dataframe_2_snowflake_parallel(insert_query, data_tuple, 1000)
    return clean_df


def clean_record(row):
    cleaned = []
    for val in row:
        # Handle NaNs
        if isinstance(val, float) and math.isnan(val):
            cleaned.append(None)
        elif isinstance(val, (np.float32, np.float64)) and np.isnan(val):
            cleaned.append(None)
        # Convert numpy integers/floats
        elif isinstance(val, (np.integer, np.floating)):
            cleaned.append(val.item())
        # Convert numpy.datetime64 or pandas.Timestamp to Python datetime
        elif isinstance(val, (np.datetime64, pd.Timestamp)):
            val = pd.to_datetime(val).to_pydatetime()
            cleaned.append(val)
        # Convert datetime.datetime to string if needed
        elif isinstance(val, datetime):
            cleaned.append(val.strftime("%Y-%m-%d"))  # or keep as datetime if supported
        else:
            cleaned.append(val)
    return tuple(cleaned)


def load_historical_credit_events_raw_docker(direct_insert: bool = False):
    # find the file
    file_list = get_file_list(
        smb_config_obj,
        base_path := os.path.join(
            "OfficialTest_AggDTD_SBChinaNA",
            "ProductionData",
            "ModelCalibration",
            "202504",
            "IDMTData",
            "SmartData",
            "FirmHistory",
            "Before_MA",
        ),
    )

    full_base_path = os.path.join(
        smb_config_obj["shared_folder"], base_path.lstrip("/")
    )

    historical_merged_df = pd.DataFrame()
    pattern = re.compile(r"^CreditEvent_\d+\.mat$")
    for item in file_list:
        if not pattern.match(item):
            continue
        full_path = os.path.join(full_base_path, item)
        res_df = load_mat_as_dataframe_old(full_path, "creditEventCRI")
        res_df.columns = [
            "CompanyID",
            "DateStr",
            "EventID",
            "DefaultType",
            "DateStr2",
            "EventID2",
        ]
        res_df["DefaultDate"] = pd.to_datetime(res_df["DateStr"], format="%Y%m%d")
        res_df = res_df.drop(columns=["DateStr"])
        res_df = res_df.where(pd.notnull(res_df), None)
        res_df = res_df.applymap(clean_cell)

        res_df = res_df[["DefaultDate", "CompanyID", "DefaultType", "EventID"]]
        res_df["AdditionalComments"] = None
        historical_merged_df = pd.concat([historical_merged_df, res_df])
    historical_merged_df = historical_merged_df.sort_values(by="DefaultDate")
    clean_df = historical_merged_df.where(pd.notnull(historical_merged_df), None)

    if direct_insert:
        # delete all data
        delete_query = "DELETE FROM CRI.CRI_PROD_MARCUS.credit_events_raw"
        pd_dataframe_2_snowflake_parallel(delete_query, ((),))

        insert_query = "INSERT INTO CRI.CRI_PROD_MARCUS.credit_events_raw (default_date, company_id, default_type, event_id, additional_comments) "
        insert_query += "VALUES (" + ", ".join(["%s" for _ in range(1, 6)]) + ") "
        # insert data to mysql
        df_obj = clean_df.astype(object)
        data_tuple = [clean_record(row) for row in df_obj.to_records(index=False)]
        for row in data_tuple:
            assert all(val == val or val is None for val in row)  # NaN fails val == val
        print("data_tuple_insert >>>", len(data_tuple))
        pd_dataframe_2_snowflake_parallel(insert_query, data_tuple, 1000)
    return clean_df


def upload_credit_events_raw_docker(
    from_date: datetime = None, to_date: datetime = None, direct_insert: bool = False
):
    current_year_start_date = datetime(datetime.now().year, 1, 1)
    current_year_start_date_str = current_year_start_date.strftime("%Y-%m-%d")

    # find the file
    file_list = get_file_list(
        smb_config_obj,
        os.path.join("/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily"),
    )

    set_of_dates = set()
    for target_date in pd.date_range(from_date, to_date):
        set_of_dates.add(target_date.strftime("%Y%m%d"))

    # print("upload_credit_events_raw_docker: 4")
    cal_files = [
        item for item in file_list if item[:8] in set_of_dates and item[:1] <= "9"
    ]  # filter recent week data and filter out file that is not related.
    # print(f"cal_files: {cal_files}")
    daily_merged_df = pd.DataFrame()
    for cal_file in cal_files:
        for regionID in range(1, 5):
            try:
                res_df = load_mat_as_dataframe_old(
                    rf"/CRI3/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily/{cal_file}/IDMTData/Clean/EconomicInformation/CompanyFlow/NewCreditEvent_Region_{regionID}.mat",
                    "newCreditEvent",
                )
                # print(f"res_df.columns: {res_df.columns}")
                # print(f"res_df.shape:   {res_df.shape}")
                # print(f"res_df.head:    {res_df.head(10)}")
                if len(res_df) == 0:
                    continue
                res_df.columns = [
                    "CompanyID",
                    "DateStr",
                    "EventID",
                    "ColExtra",
                    "DefaultType",
                    "DateStr2",
                    "EventID2",
                ]
                res_df["DefaultDate"] = pd.to_datetime(
                    res_df["DateStr"], format="%Y%m%d"
                )
                res_df = res_df.drop(columns=["DateStr"])
                res_df = res_df.where(pd.notnull(res_df), None)
                res_df = res_df.applymap(clean_cell)

                res_df = res_df[["DefaultDate", "CompanyID", "DefaultType", "EventID"]]
                res_df["AdditionalComments"] = None
                daily_merged_df = pd.concat([daily_merged_df, res_df])
            except:
                print("---- Error Loading Files ------")
                print(
                    rf"/CRI3/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily/{cal_file}/IDMTData/Clean/EconomicInformation/CompanyFlow/NewCreditEvent_Region_{regionID}.mat",
                    "newCreditEvent",
                )
                print("------------------------------- ")
    # print(f"daily_merged_df.columns: {daily_merged_df.columns}")
    # print(f"daily_merged_df.shape: {daily_merged_df.shape}")
    # print(f"daily_merged_df.head: {daily_merged_df.head(10)}")
    if "DefaultDate" in daily_merged_df:
        daily_merged_df = daily_merged_df.sort_values(by="DefaultDate")
    else:
        return daily_merged_df
    # daily_merged_df = daily_merged_df.sort_values(by="DefaultDate")
    clean_df = daily_merged_df.where(pd.notnull(daily_merged_df), None)

    if direct_insert:
        # delete all data (making sure no duplicate on insert)
        delete_query = """DELETE FROM CRI.CRI_PROD_MARCUS.credit_events_raw WHERE 
                company_id = %s AND default_date = %s AND 
                default_type = %s AND event_id = %s AND
                default_date >= %s"""
        delete_data_tuple = tuple(
            tuple(
                (
                    record["CompanyID"],
                    record["DefaultDate"],
                    record["DefaultType"],
                    record["EventID"],
                    current_year_start_date_str,
                )
            )
            for record in clean_df.to_records(index=False)
        )
        pd_dataframe_2_snowflake_parallel(delete_query, delete_data_tuple)

        insert_query = "INSERT INTO CRI.CRI_PROD_MARCUS.credit_events_raw (default_date, company_id, default_type, event_id, additional_comments) "
        insert_query += "VALUES (" + ", ".join(["%s" for _ in range(1, 6)]) + ") "
        # insert data to mysql
        df_obj = clean_df.astype(object)
        data_tuple = [clean_record(row) for row in df_obj.to_records(index=False)]
        for row in data_tuple:
            assert all(val == val or val is None for val in row)  # NaN fails val == val
        print("data_tuple_insert >>>", len(data_tuple))
        pd_dataframe_2_snowflake_parallel(insert_query, data_tuple, 1000)
    return clean_df


def upload_excluded_companies_docker(from_date: datetime = None, direct_insert: bool = False):
    from_date_str = from_date.strftime("%Y-%m-%d")  # or "%Y%m%d" to match
    res_df = pd.read_excel(
        rf"/CRI3/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Exclude_companies.xlsx"
    )
    res_df.columns = [
        "Region",
        "CompanyNumber",
        "Country",
        "Ticker",
        "CompanyName",
        "Reason",
        "Remark",
        "StartDate",
        "EndDate",
        "ErrorFoundDate",
        "SQLQuery",
        "StopReason",
        "InternalRemarks",
        "No.",
    ]
    res_df = res_df.drop(columns=["No."])
    res_df = res_df.where(pd.notnull(res_df), None)
    res_df = res_df.applymap(clean_cell)

    # filter from date
    filtered_df_all = res_df[
        (res_df["StartDate"] >= from_date_str) | (res_df["EndDate"] >= from_date_str)
    ]

    # get all ticker start date combination
    if direct_insert:
        # delete all data
        delete_query = "DELETE FROM CRI.CRI_PROD_MARCUS.excluded_companies WHERE ticker = %s AND start_date = %s"
        data_tuple_delete = tuple(
            tuple((record["Ticker"], record["StartDate"]))
            for record in filtered_df_all.to_records(index=False)
        )
        print("data_tuple_delete >>>", len(data_tuple_delete))
        pd_dataframe_2_snowflake_parallel(delete_query, data_tuple_delete, 1000)

        insert_query = "INSERT INTO CRI.CRI_PROD_MARCUS.excluded_companies (region, company_id, country_id, ticker, company_name, reason, remark, start_date, end_date, error_found_date, sql_query, stop_reason, internal_remarks) "
        insert_query += "VALUES (" + ", ".join(["%s" for _ in range(1, 14)]) + ") "
        # insert data to mysql
        data_tuple = tuple(
            tuple(record) for record in filtered_df_all.to_records(index=False)
        )
        print("data_tuple_insert >>>", len(data_tuple))
        pd_dataframe_2_snowflake_parallel(insert_query, data_tuple, 1000)
    return filtered_df_all


def get_companies_date_diff(df_1, df_2):
    # Find records in df_1 that are not in df_2
    df_1_dif_2 = df_1[
        ~df_1.set_index(["company_id", "data_date"]).index.isin(
            df_2.set_index(["company_id", "data_date"]).index
        )
    ]
    df_1_dif_2 = df_1_dif_2.drop_duplicates()

    # Find records in df_2 that are not in df_1
    df_2_dif_1 = df_2[
        ~df_2.set_index(["company_id", "data_date"]).index.isin(
            df_1.set_index(["company_id", "data_date"]).index
        )
    ]
    df_2_dif_1 = df_2_dif_1.drop_duplicates()

    return (
        df_1_dif_2[["company_id", "data_date"]],
        df_2_dif_1[["company_id", "data_date"]],
    )

def pd_daily_sf(date=None):

    if (
        date == None
    ):  # if it is a daily automate task, then read t-2 data. else read data of the target date.
        date = datetime.today() - timedelta(days=2)
    else:
        date = datetime.strptime(date, "%Y-%m-%d")

    operation_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    cal_file, calibration_date = get_calibration_date(date)
    data_date = str(cal_file[:8])
    print(f"calibration_date: {calibration_date}, data_date: {data_date}")

    # cleanup_temp_folder()

    remote_path = os.path.join(
        "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily/"
        + cal_file
        + "/Products/P2_Pd"
    )
    sub_file_list = get_file_list(smb_config_obj, remote_path)
    # print(sub_file_list)

    # #download the file
    # connect_and_download_file(smb_config_obj, sub_file_list, local_file_path, remote_path, 0)
    # ---------------------------------------------------------------------------------------------------------------------------------------
    mat_data = []
    mat_files = [
        os.path.join(
            smb_config_obj["shared_folder"]+
            remote_path + "/" +
            item
        )
        for item in sub_file_list
        if item.startswith("pd60h_")
    ]
    # print(f"smb_config_obj[shared_folder]: {smb_config_obj["shared_folder"]}")
    # print(mat_files)
    print(f"Current Date: {date}, sub_file_list: {len(mat_files)}")

    pd_econs_covered = set()
    for mat_file in mat_files:
        try:
            data = loadmat(mat_file)
            mat_data.append(data)

            econ_id = mat_file.split("_")[-1].split(".")[0]
            pd_econs_covered.add(econ_id)
        except Exception as e:
            print(f"Error loading the file '{mat_file}': {e}")
    print(sub_file_list[:2])
    print(mat_files[:2])

    df = daily_data_preprocessing(mat_data, calibration_date, operation_time)
    df["data_date"] = date.strftime("%Y-%m-%d")
    df["current_trading_date"] = int(data_date)
    print(f"DATE: {date}, Len: {len(df)}")
    print(df["data_date"].unique())

    # get df_date_company_id
    df_date_company_id = df[["data_date", "company_id"]]
    df_date_company_id = df_date_company_id.astype(str)

    (pdir_df, pdir_econs_covered) = pdir_daily(
        date=date,
        df_date_company_id=df_date_company_id,
    )

    df_pd_dif_pdir, df_pdir_dif_pd = get_companies_date_diff(df, pdir_df)

    # print number of rows in df & pdir_df before merge
    print("*=" * 50)
    print(f"(BEFORE MERGE) Number of rows in df: {len(df)}")
    print(f"(BEFORE MERGE) Number of rows in pdir_df: {len(pdir_df)}")

    # merge df & pdir_df (left join)
    df = df.merge(pdir_df, on=["data_date", "company_id"], how="left")
    # pdir
    df["PDIR"] = df["pdir_extracted"]
    df = df.drop(columns=["pdir_extracted"])

    # print number of rows in df & pdir_df after merge
    print(f"(AFTER MERGE) Number of rows in df: {len(df)}")
    print(f"(AFTER MERGE) Diff: {len(df) - len(pdir_df)}")
    print("-" * 50)
    print(f"df_pd_dif_pdir: {df_pd_dif_pdir.head()}")
    print(f"df_pdir_dif_pd: {df_pdir_dif_pd.head()}")
    print("-" * 50)
    print(f"PD ECONS COVERED: {len(pd_econs_covered)}")
    print(f"PDIR ECONS COVERED: {len(pdir_econs_covered)}")
    print(f"PD ECONS - PDIR ECONS: {pd_econs_covered - pdir_econs_covered}")
    print(f"PDIR ECONS - PD ECONS: {pdir_econs_covered - pd_econs_covered}")
    print("*=" * 50)

    return df
