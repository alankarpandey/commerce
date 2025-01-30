import logging
import os
from datetime import datetime

# Define current date for folder name
current_date = datetime.now().strftime('%Y_%m_%d')

# Create logs directory with date-based folder
logs_path = os.path.join(os.getcwd(), "logs", current_date)
os.makedirs(logs_path, exist_ok=True)

# Define log file path
LOG_FILE = f"{datetime.now().strftime('%H_%M_%S')}.log"
LOG_FILE_PATH = os.path.join(logs_path, LOG_FILE)

# Configure logging
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="%(asctime)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Example log message
logging.info("Log setup complete.")