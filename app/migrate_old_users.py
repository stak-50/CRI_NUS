import json
import uuid
from src.modules import pd_dataframe_2_snowflake_parallel, run_sf_select_query
from src.utils import APP_DIR

USERS_JSON_FILE_PATH = rf"{APP_DIR}\users.json"

# Load the JSON file
with open(USERS_JSON_FILE_PATH, "r") as file:
    users_data = json.load(file)


# Prepare mappings and conversions
def convert_to_bool(value):
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if value is None:
        return "FALSE"
    if isinstance(value, str):
        return "TRUE" if value.upper() == "Y" else "FALSE"
    if isinstance(value, int):
        return "TRUE" if value == 1 else "FALSE"
    return "FALSE"


def format_str(value):
    if value is None:
        return "NULL"
    safe_value = str(value).replace("'", "''")
    return f"{safe_value}"


def format_timestamp(value):
    if value is None:
        return "NULL"
    return value  # Already in 'YYYY-MM-DD HH:MM:SS' format


# Build the VALUES part
data_tuple = []
for user in users_data:
    values = (
        str(uuid.uuid4()),  # ID
        f"{format_str(user.get('Email'))}",  # EMAIL
        f"{format_str(user.get('FirstName'))}",  # FIRST_NAME
        f"{format_str(user.get('LastName'))}",  # LAST_NAME
        f"{format_str(user.get('Company'))}",  # COMPANY
        f"{format_str(user.get('Designation'))}",  # DESIGNATION
        f"{format_str(user.get('Department'))}",  # DEPARTMENT
        f"{format_str(user.get('Industry'))}",  # INDUSTRY
        f"{format_str(user.get('Phone'))}",  # PHONE
        f"{format_str(user.get('Type', 'REGULAR'))}",  # TYPE
        f"{format_str(user.get('Status', 'UNACTIVATED'))}",  # STATUS
        f"{format_str(user.get('ActivationCode'))}",  # ACTIVATION_CODE
        f"{format_str(user.get('AdditionalComments'))}",  # ADDITIONAL_COMMENTS
        f"{convert_to_bool(user.get('GlobalCompanyAccess'))}",  # GLOBAL_COMPANY_ACCESS
        f"{format_str(user.get('ClientAccess'))}",  # CLIENTS_ACCESS
        f"{convert_to_bool(user.get('SecondLevelAgreement'))}",  # SECOND_LEVEL_AGREEMENT
        f"{format_str(user.get('Hashpassword'))}",  # HASH_PASSWORD
        f"{format_str(user.get('Hashtoken'))}",  # HASH_TOKEN
        f"{format_timestamp(user.get('Created'))}",  # CREATED
        f"{format_timestamp(user.get('Updated'))}",  # UPDATED
        f"{convert_to_bool(user.get('receive_occasional', False))}",  # RECEIVE_OCCASIONAL
        f"{convert_to_bool(user.get('receive_periodic', False))}",  # RECEIVE_PERIODIC
        f"{convert_to_bool(1)}",  # CRI_DASHBOARD_ACCESS (TODO: by default I'm enabling all users to access CRI_DASHBOARD)
        f"{convert_to_bool(user.get('BudaAccess'))}",  # BUDA_ACCESS
        f"{format_str(user.get('SpecialRemarks'))}",  # SPECIAL_REMARKS
        f"{format_timestamp(user.get('ExpiryDate'))}",  # EXPIRY_DATE
        f"{convert_to_bool(user.get('BuDAPopUpDisable'))}",  # BUDA_POPUP_DISABLE
        None,  # LAST_LOGIN (timestamp)
    )
    data_tuple.append(values)

# Final INSERT statement
insert_query = f"""
INSERT INTO CRI.CRI_AUTH.CRI_USER (
    ID, EMAIL, FIRST_NAME, LAST_NAME, COMPANY, DESIGNATION, DEPARTMENT,
    INDUSTRY, PHONE, TYPE, STATUS, ACTIVATION_CODE, ADDITIONAL_COMMENTS,
    GLOBAL_COMPANY_ACCESS, CLIENTS_ACCESS, SECOND_LEVEL_AGREEMENT, HASH_PASSWORD,
    HASH_TOKEN, CREATED, UPDATED, RECEIVE_OCCASIONAL, RECEIVE_PERIODIC,
    CRI_DASHBOARD_ACCESS, BUDA_ACCESS, SPECIAL_REMARKS, EXPIRY_DATE,
    BUDA_POPUP_DISABLE, LAST_LOGIN
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s
)
"""

# Fetch existing emails
emails = run_sf_select_query(f"SELECT EMAIL FROM CRI.CRI_AUTH.CRI_USER")
existing_emails = {row[0] for row in emails}

data_tuple_filtered = [row for row in data_tuple if row[1] not in existing_emails]

if len(data_tuple_filtered) > 0:
    pd_dataframe_2_snowflake_parallel(insert_query, data_tuple_filtered, 1000)
