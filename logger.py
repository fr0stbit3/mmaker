import os
import logging
from logging.handlers import RotatingFileHandler


os.environ["TZ"] = "Asia/Kolkata"
handler = RotatingFileHandler("mmaker.log", maxBytes=52428800, backupCount=10)
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", handlers=[handler])
logger = logging.getLogger()
logger.setLevel(logging.INFO)
