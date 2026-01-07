import pandas as pd
import numpy as np
from .helper import *
from datetime import datetime, timedelta, timezone
import scipy.io
from src.utils import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from functools import partial


def as_yearly_historical(yyyymm, start_econ_id=1, end_econ_id=200):
    """
    insert the 1988 - (current_year - 1) data into as_company_individual_daily_complete_latest
    """

    create_tmp_folder()

    insert_query = (
        "INSERT IGNORE INTO cri_pd_prod_marcus.as_company_individual_daily_complete_latest (data_date, econ_id, company_id, calibration_date, operation_time, "
        + ", ".join(["cdsvalue" + str(i) for i in range(1, 6)])
        + ") "
    )
    insert_query += "VALUES (" + ", ".join(["%s" for _ in range(1, 11)]) + ") "

    finish_econ_id_list = []
    remote_paths = [
        os.path.join(
            "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Historical/"
            + yyyymm
            + "/Daily/Products/P7_As/"
            + f"individualAS_1990-01-01_2023-12-29/{s}"
        )
        for s in range(start_econ_id, end_econ_id + 1)
    ]

    for remote_path in remote_paths:
        # get econ_id from remote_path
        econ_id = os.path.split(remote_path)[1]

        if econ_id not in finish_econ_id_list:

            print("start process econ id: ", econ_id)

            while True:
                # get the list of file
                file_list = get_file_list(smb_config_obj, remote_path)

                if not file_list:
                    break

                # filter out not expected file
                file_list = [item for item in file_list if item.startswith("AS_")]
                initial_file_list_len = len(file_list)
                print("num of files: ", initial_file_list_len)

                # inserted_data = inserted_company_and_date(econ_id, metric_type='as')

                file_amt_limit = 30

                # entire pipeline
                while len(file_list) > 0:

                    # delete all old temp doc in folder
                    cleanup_temp_folder()

                    # prevent machine out-of-memory (perform download and insert tasks batch by batch)
                    sub_file_list = []

                    for _ in range(file_amt_limit):
                        if file_list:  # not null
                            sub_file_list.append(file_list.pop())

                    # download the file
                    connect_and_download_file(
                        smb_config_obj, sub_file_list, local_file_path, remote_path, 0
                    )
                    # ---------------------------------------------------------------------------------------------------------------------------------------
                    csv_data = pd.DataFrame()
                    csv_files = glob.glob(os.path.join("tmp", "*.csv"))

                    for csv_file in csv_files:
                        try:
                            csv_data = pd.concat(
                                [csv_data, pd.read_csv(csv_file, index_col=None)],
                                ignore_index=True,
                            )
                        except pd.errors.EmptyDataError:
                            print(f"Empty file: {csv_file}")

                    if len(csv_data) == 0:
                        continue
                    df = as_data_preprocessing(csv_data, econ_id=econ_id)
                    # ---------------------------------------------------------------------------------------------------------------------------------------
                    # remove already inserted company data (prevent duplicated insertion) (check both company_id and date)
                    # df = df[df.apply(lambda row: check_not_inserted(row, inserted_data), axis=1)]

                    # pandas DF to tuple
                    data_tuple = tuple(
                        tuple(record) for record in df.to_records(index=False)
                    )

                    # insert data to mysql
                    pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

                    progress = (1 - len(file_list) / initial_file_list_len) * 100
                    print("econ id: %s, progress: %.2f%%" % (econ_id, progress))

                print("finish inserting", remote_path)
                finish_econ_id_list.append(econ_id)
                print("finished econ id:", finish_econ_id_list)
                break

        else:

            print("already inserted econ : %s, will not process again" % (econ_id))


def as_process_batch(csv_files, operation_time, yyyymm, econ_id):
    csv_data = pd.DataFrame()
    for csv_file in csv_files:
        try:
            csv_data = pd.concat(
                [csv_data, pd.read_csv(csv_file, index_col=None)], ignore_index=True
            )
        except pd.errors.EmptyDataError:
            print(f"Empty file: {csv_file}")

    if len(csv_data) == 0:
        return

    try:
        df = as_data_preprocessing(csv_data, operation_time, operation_time)
        random_string = "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        )
        # Get current time in milliseconds
        timestamp_ms = int(time.time() * 1000)
        res = save_data_as_parquet(
            df,
            rf"{APP_DIR}\as_parquet_data\as_{yyyymm}_{econ_id}_{random_string}_{timestamp_ms}",
        )
        print("Data Saved!!")
        if not res:
            print(f"Error saving data as parquet")
            # save data as csv
            df.to_csv(
                rf"{APP_DIR}\as_failed_data\as_{yyyymm}_{econ_id}.csv",
                index=False,
            )
    except Exception as e:
        print(f"Error Processing Batch {e}")


def as_run_parallel_batches(
    files_batches, operation_time, yyyymm, econ_id, max_workers=4
):
    total_batches = len(files_batches)
    completed_batches = 0
    lock = Lock()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        process_fn = partial(
            as_process_batch,
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


def as_yearly_historical(yyyymm, start_econ_id=1, end_econ_id=200, file_amt_limit=50):
    """
    insert the 1988 - (current_year - 1) data into pd_company_individual_daily_historical
    """

    create_tmp_folder("as_parquet_data")
    create_tmp_folder("as_failed_data")

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
            + "/Daily/Products/P7_As/"
            + f"individualAS_1990-01-01_2023-12-29/{s}"
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
                    if item.startswith("AS_")
                ]
                initial_file_list_len = len(file_list)
                print(f"Econ Id: {econ_id} num of csv files: {initial_file_list_len}")

                files_batches = [
                    file_list[i : i + file_amt_limit]
                    for i in range(0, len(file_list), file_amt_limit)
                ]

                as_run_parallel_batches(
                    files_batches, operation_time, yyyymm, econ_id, 5
                )

                print("finish inserting", remote_path)
                finish_econ_id_list.append(econ_id)
                print("finished econ id:", finish_econ_id_list)
                break
        else:
            print("already inserted econ : %s, will not process again" % (econ_id))


def as_daily(date=None, duplication_check=True, insert_daily_table=True):
    create_tmp_folder()

    if (
        date == None
    ):  # if it is a daily automate task, then read t-2 data. else read data of the target date.
        date = datetime.today() - timedelta(days=2)
    else:
        date = datetime.strptime(date, "%Y-%m-%d")

    operation_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    cal_file, calibration_date = get_calibration_date(date)
    # check if already input before
    if duplication_check:
        ifexist = execute_sql_query(
            "select 1 from cri_pd_prod_marcus.as_company_individual_daily_operation_log where operation_action = 'daily_insertion' and data_end_date = '%s' union all select 0 limit 1"
            % (date.strftime("%Y-%m-%d")),
            operation_type="select",
        ).iloc[0, 0]
        if ifexist == 1:
            return "data already inserted before"

    data_date = str(cal_file[:8])
    calibration_date = str(cal_file[-8:])

    cleanup_temp_folder()

    remote_path = os.path.join(
        "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Recent/Daily/"
        + cal_file
        + "/Products/P7_As"
    )
    sub_file_list = get_file_list(smb_config_obj, remote_path)
    sub_file_list = [item for item in sub_file_list if item.startswith("as_")]

    # download the file
    connect_and_download_file(
        smb_config_obj, sub_file_list, local_file_path, remote_path, 0
    )
    # ---------------------------------------------------------------------------------------------------------------------------------------
    mat_data = []
    mat_files = glob.glob(os.path.join("tmp", "*.mat"))

    for mat_file in mat_files:
        try:
            data = loadmat(mat_file)
            mat_data.append(data)
        except Exception as e:
            print(f"Error loading the file '{mat_file}': {e}")
    if len(mat_data) == 0:
        return "no data found"

    df = as_daily_data_preprocessing(mat_data, calibration_date, operation_time)
    # ---------------------------------------------------------------------------------------------------------------------------------------
    # pandas DF to tuple
    data_tuple = tuple(tuple(record) for record in df.to_records(index=False))

    # insert data to daily table
    insert_query = (
        "INSERT IGNORE INTO cri_pd_prod_marcus.as_company_individual_daily_1d (data_date, econ_id, company_id, calibration_date, operation_time, "
        + ", ".join(["cdsvalue" + str(i) for i in range(1, 6)])
        + ") "
    )
    insert_query += "VALUES (" + ", ".join(["%s" for _ in range(10)]) + ") "
    if insert_daily_table:
        execute_sql_query(
            """delete from cri_pd_prod_marcus.as_company_individual_daily_1d""",
            operation_type="change",
        )  # delete old data
        pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

    # insert overwrite data into latest complete table
    insert_query = (
        "INSERT IGNORE INTO cri_pd_prod_marcus.as_company_individual_daily_complete_latest (data_date, econ_id, company_id, calibration_date, operation_time, "
        + ", ".join(["cdsvalue" + str(i) for i in range(1, 6)])
        + ") "
    )
    insert_query += "VALUES (" + ", ".join(["%s" for _ in range(10)]) + ") "
    pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

    # add activity into operation log
    data_date = date.strftime("%Y-%m-%d")
    calibration_date = datetime.strptime(calibration_date, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )

    execute_sql_query(
        """
    INSERT INTO cri_pd_prod_marcus.as_company_individual_daily_operation_log 
    (data_start_date, data_end_date, econ_id, company_id, calibration_date, operation_time, operation_action) 
    VALUES ('%s', '%s', '-99', '-99', '%s', '%s', 'daily_insertion');
"""
        % (data_date, data_date, calibration_date, operation_time),
        operation_type="insert",
    )


def as_revision(date=None, duplication_check=True, insert_daily_table=True):
    create_tmp_folder()

    if (
        date == None
    ):  # if it is a daily automate task, then read t-6 data. else read data of the target date.
        date = datetime.today() - timedelta(days=6)
    else:
        date = datetime.strptime(date, "%Y-%m-%d")

    operation_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # find the file
    file_list = get_file_list(
        smb_config_obj,
        os.path.join("/OfficialTest_AggDTD_SBChinaNA/ProductionData/Historical"),
    )
    file = [
        item
        for item in file_list
        if item[:8] == date.strftime("%Y%m%d") and item[:1] < "9" and len(item) == 8
    ]  # filter recent week data and filter out file that is not related.

    if len(file) == 0:
        return "Revision: no cal file for your target date: %s, no data inserted." % (
            date
        )
    if len(file) > 1:
        return "more than one cal file for your target date: %s, no data inserted." % (
            date
        )

    file = file[0]

    # check if already input before
    if duplication_check:
        check_date = date - timedelta(
            days=15
        )  # to check if revision data has been inserted

        ifexist = execute_sql_query(
            "select 1 from cri_pd_prod_marcus.as_company_individual_daily_operation_log where operation_action = 'revision' and data_end_date between '%s' and '%s' union all select 0 limit 1"
            % (check_date.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d")),
            operation_type="select",
        ).iloc[0, 0]
        if ifexist == 1:
            return "data already inserted before"

    data_date = str(file[:8])
    calibration_date = execute_sql_query(
        "select calibration_date from cri_pd_prod_marcus.as_company_individual_daily_operation_log where operation_action = 'daily_insertion' and data_start_date >= '%s' order by data_start_date asc limit 1"
        % (date.strftime("%Y-%m-%d")),
        operation_type="select",
    ).iloc[0][0]

    remote_paths = [
        os.path.join(
            "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Historical/"
            + file
            + "/Daily/Products/P7_As/"
            + f"IndividualAS/{s}"
        )
        for s in range(1, 200 + 1)
    ]
    for remote_path in remote_paths:
        print("remote_path", remote_path)
        cleanup_temp_folder()

        sub_file_list = get_file_list(smb_config_obj, remote_path)
        sub_file_list = [item for item in sub_file_list if item.startswith("AS_")]
        # download the file
        connect_and_download_file(
            smb_config_obj, sub_file_list, local_file_path, remote_path, 0
        )
        # ---------------------------------------------------------------------------------------------------------------------------------------
        csv_data = pd.DataFrame()
        csv_files = glob.glob(os.path.join("tmp", "*.csv"))

        for csv_file in csv_files:
            csv_data = pd.concat(
                [csv_data, pd.read_csv(csv_file, index_col=None)], ignore_index=True
            )
        if len(csv_data) == 0:
            continue

        df = as_data_preprocessing(csv_data, calibration_date, operation_time)
        # ---------------------------------------------------------------------------------------------------------------------------------------
        if df.shape[0] <= 0:
            continue

        # pandas DF to tuple
        data_tuple = tuple(tuple(record) for record in df.to_records(index=False))

        # insert old data to incremental table as_company_individual_daily_incremental
        insert_query = (
            "INSERT INTO cri_pd_prod_marcus.as_company_individual_daily_incremental (data_date, econ_id, company_id, calibration_date, operation_time, "
            + ", ".join(["cdsvalue" + str(i) for i in range(1, 6)])
            + ") "
        )
        insert_query += (
            "select data_date, econ_id, company_id, calibration_date, operation_time, "
            + ", ".join(["cdsvalue" + str(i) for i in range(1, 6)])
            + " from cri_pd_prod_marcus.as_company_individual_daily_complete_latest where data_date = %s and company_id = %s"
        )
        data_tuple_incre = tuple((record[0], record[2]) for record in data_tuple)

        pd_dataframe_2_mysql(insert_query, data_tuple_incre, 1000)

        # insert overwrite data into latest complete table
        insert_query = (
            "INSERT INTO cri_pd_prod_marcus.as_company_individual_daily_complete_latest (data_date, econ_id, company_id, calibration_date, operation_time, "
            + ", ".join(["cdsvalue" + str(i) for i in range(1, 6)])
            + ") "
        )
        insert_query += "VALUES (" + ", ".join(["%s" for _ in range(10)]) + ") "
        insert_query += (
            "ON DUPLICATE KEY UPDATE "
            + "econ_id = VALUES(econ_id), calibration_date = VALUES(calibration_date), operation_time = VALUES(operation_time), "
        )
        insert_query += ", ".join(
            ["cdsvalue%s = VALUES(cdsvalue%s)" % (i, i) for i in range(1, 6)]
        )
        pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

        # insert data to daily table
        daily_df = df[df["data_date"] == date.strftime("%Y-%m-%d")]
        data_tuple = tuple(tuple(record) for record in daily_df.to_records(index=False))
        insert_query = (
            "INSERT INTO cri_pd_prod_marcus.as_company_individual_daily_1d (data_date, econ_id, company_id, calibration_date, operation_time, "
            + ", ".join(["cdsvalue" + str(i) for i in range(1, 6)])
            + ") "
        )
        insert_query += "VALUES (" + ", ".join(["%s" for _ in range(10)]) + ") "
        insert_query += (
            "ON DUPLICATE KEY UPDATE "
            + "econ_id = VALUES(econ_id), calibration_date = VALUES(calibration_date), operation_time = VALUES(operation_time), "
        )
        insert_query += ", ".join(
            ["cdsvalue%s = VALUES(cdsvalue%s)" % (i, i) for i in range(1, 6)]
        )
        if insert_daily_table:
            pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

        # add activity into operation log
        insert_query = """
        INSERT INTO cri_pd_prod_marcus.as_company_individual_daily_operation_log 
        (data_start_date, data_end_date, econ_id, company_id, calibration_date, operation_time, operation_action) 
        VALUES (%s, %s, %s, %s, %s, %s, %s);"""

        operation_df = revision_operation_df(df)
        data_tuple = tuple(
            tuple(record) for record in operation_df.to_records(index=False)
        )

        pd_dataframe_2_mysql(insert_query, data_tuple, 1000)
