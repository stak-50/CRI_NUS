from src.modules import *
from src.as_modules import *
import snowflake.connector
import sys
import logging
from logging.handlers import RotatingFileHandler

# Setup rotating log file
log_file = "as_snowflake_migration.log"
log_formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")

handler = RotatingFileHandler(log_file, maxBytes=100 * 1024 * 1024, backupCount=5)
handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


# Redirect print → logger
class PrintLogger:
    def __init__(self, logger_func):
        self.logger_func = logger_func

    def write(self, message):
        message = message.strip()
        if message:
            self.logger_func(message)

    def flush(self):
        pass  # Needed for compatibility


sys.stdout = PrintLogger(logger.info)
sys.stderr = PrintLogger(logger.error)

# TODO: change date accordingly (i.e. from past how many months of data to be refreshed)
start_data_date = "2024-12-31"

if __name__ == "__main__":
    try:
        yyyymm = start_data_date[0:7].replace("-", "")
        print(yyyymm)
        print("Starting snowflake migration...")
        print(pd_yearly_historical(yyyymm, 1, 200))
        # print(as_yearly_historical(yyyymm, 1, 2))
    except Exception as e:
        print(f"ERROR:::::Script failed with error: {e}")
