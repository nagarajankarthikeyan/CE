# logging_config.py
import logging
import os
from datetime import datetime

LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)


class SimpleFormatter(logging.Formatter):
    def format(self, record):
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            level = record.levelname
            msg = record.getMessage()
            return f"{ts} | {level:<5} | {msg}"
        except Exception:
            return record.getMessage()


def get_log_file_name() -> str:
    """Generate date-wise log filename: app_YYYYMMDD.log"""
    date_str = datetime.now().strftime("%Y%m%d")
    return os.path.join(LOGS_DIR, f"app_{date_str}.log")


def setup_logging():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()

    formatter = SimpleFormatter()

    # 🔹 Console Handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    root_logger.addHandler(console)

    # 🔹 Date-wise File Handler (no rotation, new file every day)
    log_file_path = get_log_file_name()
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    return root_logger


# Export logger
app_logger = setup_logging()
