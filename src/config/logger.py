import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone

# --- Configuration ---
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_FILE = os.path.join(LOG_DIR, "app.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 5

os.makedirs(LOG_DIR, exist_ok=True)


# --- Formatters ---
class UTCFormatter(logging.Formatter):
    """Custom formatter that enforces UTC timestamps."""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime(datefmt) if datefmt else dt.isoformat()


formatter = UTCFormatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z",
)


# --- Handlers ---
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=MAX_BYTES,
    backupCount=BACKUP_COUNT,
    encoding="utf-8",
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)


# --- Root Logger Setup ---
def setup_logging():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        handlers=[file_handler, console_handler],
    )

    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logging.critical(
            "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)
        )

    sys.excepthook = handle_exception

    class StreamToLogger:
        def __init__(self, logger, level):
            self.logger = logger
            self.level = level

        def write(self, buf):
            for line in buf.rstrip().splitlines():
                self.logger.log(self.level, line.rstrip())

        def flush(self):
            pass

    sys.stderr = StreamToLogger(logging.getLogger("STDERR"), logging.ERROR)

    # --- SQLAlchemy Logging ---
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.WARNING
    )  # queries (set to INFO for full SQL logs)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)  # DB pool events
    logging.getLogger("sqlalchemy.dialects").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.orm").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.ERROR)  # only errors

    # Optional: if you want SQL statements logged, uncomment this:
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


setup_logging()
