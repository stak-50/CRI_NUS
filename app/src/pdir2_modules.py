import pandas as pd
import numpy as np
from src.helper import *
from datetime import datetime, timedelta, timezone
import scipy.io


def pdir2_yearly_historical(yyyymm, start_econ_id=1, end_econ_id=200):
    """
    insert the 1988 - (current_year - 1) data into pdir2_company_individual_daily_historical
    """

    create_tmp_folder()

    insert_query = "INSERT IGNORE INTO cri_pd_prod_marcus.pdir2_company_individual_daily_complete_latest (data_date, econ_id, company_id, calibration_date, operation_time, pdir) "
    insert_query += "VALUES (" + ", ".join(["%s" for _ in range(6)]) + ") "

    remote_path = os.path.join(
        "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Historical/"
        + yyyymm
        + "/Daily/Products/P17_Pdir2.0"
    )

    operation_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for econ_id in range(start_econ_id, end_econ_id + 1):
        print("start processing econ_id: ", econ_id)
        sub_file_list = get_file_list(smb_config_obj, remote_path)
        sub_file_list = [
            item
            for item in sub_file_list
            if item.split("_")[-1].replace(".mat", "") == str(econ_id)
        ]
        cleanup_temp_folder()

        # download the file
        connect_and_download_file(
            smb_config_obj, sub_file_list, local_file_path, remote_path, 0
        )
        # ---------------------------------------------------------------------------------------------------------------------------------------
        mat_files = glob.glob(os.path.join("tmp", "resultRating_*.mat"))

        mat_data = pdir_historical_mat_2_pd(mat_files)

        if len(mat_data) == 0:
            continue

        df = pdir_historical_data_preprocessing(mat_data, operation_time, yyyymm)
        # ---------------------------------------------------------------------------------------------------------------------------------------
        # pandas DF to tuple
        data_tuple = tuple(tuple(record) for record in df.to_records(index=False))

        # insert data to mysql
        pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

        print("completed econ_id: ", econ_id)


def pdir2_daily(date=None, duplication_check=True, insert_daily_table=True):
    create_tmp_folder()

    if (
        date == None
    ):  # if it is a daily automate task, then read t-3 data. else read data of the target date.
        date = datetime.today() - timedelta(days=2)
    else:
        date = datetime.strptime(date, "%Y-%m-%d")

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
    # check if already input before
    if duplication_check:
        ifexist = execute_sql_query(
            "select 1 from cri_pd_prod_marcus.pdir2_company_individual_daily_operation_log where operation_action = 'daily_insertion' and data_end_date = '%s' union all select 0 limit 1"
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
        + "/Products/P17_Pdir2.0"
    )
    sub_file_list = get_file_list(smb_config_obj, remote_path)
    sub_file_list = [item for item in sub_file_list if item.startswith("pdirNum_")]

    # download the file
    connect_and_download_file(
        smb_config_obj, sub_file_list, local_file_path, remote_path, 0
    )
    # ---------------------------------------------------------------------------------------------------------------------------------------
    mat_data = []
    mat_files = glob.glob(os.path.join("tmp", "*.mat"))

    for mat_file in mat_files:
        try:
            data = h5py.File(mat_file, "r")
            mat_data.append(data)
        except Exception as e:
            print(f"Error loading the file '{mat_file}': {e}")

    if len(mat_data) == 0:
        return "no data found"

    df = pdir_daily_data_preprocessing(
        mat_data, calibration_date, operation_time, transpose=True
    )
    # ---------------------------------------------------------------------------------------------------------------------------------------
    # pandas DF to tuple
    data_tuple = tuple(tuple(record) for record in df.to_records(index=False))

    # insert data to daily table
    insert_query = "INSERT IGNORE INTO cri_pd_prod_marcus.pdir2_company_individual_daily_1d (data_date, econ_id, company_id, calibration_date, operation_time, pdir)"
    insert_query += "VALUES (" + ", ".join(["%s" for _ in range(6)]) + ") "
    if insert_daily_table:
        execute_sql_query(
            """delete from cri_pd_prod_marcus.pdir2_company_individual_daily_1d""",
            operation_type="change",
        )  # delete old data
        pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

    # insert overwrite data into latest complete table
    insert_query = "INSERT IGNORE INTO cri_pd_prod_marcus.pdir2_company_individual_daily_1d (data_date, econ_id, company_id, calibration_date, operation_time, pdir)"
    insert_query += "VALUES (" + ", ".join(["%s" for _ in range(6)]) + ") "
    insert_query = insert_query.replace(
        "pdir2_company_individual_daily_incremental",
        "pdir2_company_individual_daily_complete_latest",
    )
    pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

    # add activity into operation log
    data_date = date.strftime("%Y-%m-%d")
    calibration_date = datetime.strptime(calibration_date, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )

    execute_sql_query(
        """
    INSERT INTO cri_pd_prod_marcus.pdir2_company_individual_daily_operation_log 
    (data_start_date, data_end_date, econ_id, company_id, calibration_date, operation_time, operation_action) 
    VALUES ('%s', '%s', '-99', '-99', '%s', '%s', 'daily_insertion');
"""
        % (data_date, data_date, calibration_date, operation_time),
        operation_type="insert",
    )


def pdir2_revision(date=None, duplication_check=True, insert_daily_table=True):
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
            "select 1 from cri_pd_prod_marcus.pdir2_company_individual_daily_operation_log where operation_action = 'revision' and data_end_date between '%s' and '%s' union all select 0 limit 1"
            % (check_date.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d")),
            operation_type="select",
        ).iloc[0, 0]
        if ifexist == 1:
            return "data already inserted before"

    data_date = str(file[:8])
    calibration_date = execute_sql_query(
        "select calibration_date from cri_pd_prod_marcus.pdir2_company_individual_daily_operation_log where operation_action = 'daily_insertion' and data_start_date >= '%s' order by data_start_date asc limit 1"
        % (date.strftime("%Y-%m-%d")),
        operation_type="select",
    ).iloc[0][0]

    remote_path = (
        "/OfficialTest_AggDTD_SBChinaNA/ProductionData/Historical/"
        + file
        + "/Daily/Products/P17_Pdir2.0"
    )

    cleanup_temp_folder()

    sub_file_list = get_file_list(smb_config_obj, remote_path)
    sub_file_list = [item for item in sub_file_list if item.startswith("result")]
    # download the file
    connect_and_download_file(
        smb_config_obj, sub_file_list, local_file_path, remote_path, 0
    )
    # ---------------------------------------------------------------------------------------------------------------------------------------
    mat_files = glob.glob(os.path.join("tmp", "resultRating_*.mat"))

    mat_data = pdir_revision_mat_2_pd(mat_files, pdir=2)
    if len(mat_data) == 0:
        return "no data found"

    df = pdir_revision_data_preprocessing(mat_data, calibration_date, operation_time)
    # ---------------------------------------------------------------------------------------------------------------------------------------
    # pandas DF to tuple
    data_tuple = tuple(tuple(record) for record in df.to_records(index=False))

    # insert data to incremental table pdir2_company_individual_daily_incremental
    insert_query = "INSERT INTO cri_pd_prod_marcus.pdir2_company_individual_daily_incremental (data_date, econ_id, company_id, calibration_date, operation_time, pdir) "
    insert_query += "select data_date, econ_id, company_id, calibration_date, operation_time, pdir from cri_pd_prod_marcus.pdir2_company_individual_daily_complete_latest where data_date = %s and company_id = %s"

    data_tuple_incre = tuple((record[0], record[2]) for record in data_tuple)
    pd_dataframe_2_mysql(insert_query, data_tuple_incre, 1000)

    # insert overwrite data into latest complete table
    insert_query = "INSERT INTO cri_pd_prod_marcus.pdir2_company_individual_daily_incremental (data_date, econ_id, company_id, calibration_date, operation_time, pdir) "
    insert_query += "VALUES (" + ", ".join(["%s" for _ in range(6)]) + ") "
    insert_query += (
        "ON DUPLICATE KEY UPDATE "
        + "econ_id = VALUES(econ_id), calibration_date = VALUES(calibration_date), operation_time = VALUES(operation_time), "
    )
    insert_query += "pdir = VALUES(pdir)"
    insert_query = insert_query.replace(
        "pdir2_company_individual_daily_incremental",
        "pdir2_company_individual_daily_complete_latest",
    )
    pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

    # insert data to daily table
    daily_df = df[df["data_date"] == date.strftime("%Y-%m-%d")]
    data_tuple = tuple(tuple(record) for record in daily_df.to_records(index=False))
    insert_query = "INSERT INTO cri_pd_prod_marcus.pdir2_company_individual_daily_1d (data_date, econ_id, company_id, calibration_date, operation_time, pdir) "
    insert_query += "VALUES (" + ", ".join(["%s" for _ in range(6)]) + ") "
    insert_query += (
        "ON DUPLICATE KEY UPDATE "
        + "econ_id = VALUES(econ_id), calibration_date = VALUES(calibration_date), operation_time = VALUES(operation_time), "
    )
    insert_query += "pdir = VALUES(pdir)"
    if insert_daily_table:
        pd_dataframe_2_mysql(insert_query, data_tuple, 1000)

    # add activity into operation log
    insert_query = """
    INSERT INTO cri_pd_prod_marcus.pdir2_company_individual_daily_operation_log 
    (data_start_date, data_end_date, econ_id, company_id, calibration_date, operation_time, operation_action) 
    VALUES (%s, %s, %s, %s, %s, %s, %s);"""

    operation_df = revision_operation_df(df)
    data_tuple = tuple(tuple(record) for record in operation_df.to_records(index=False))

    pd_dataframe_2_mysql(insert_query, data_tuple, 1000)
