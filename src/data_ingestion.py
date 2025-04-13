import os
import pandas as pd
from sklearn.model_selection import train_test_split
from src.logger import get_logger
from src.custom_exception import CustomException
from config.paths_config import *
from utils.common_functions import read_yaml

logger = get_logger(__name__)

class DataIngestion:
    def __init__(self, config):   # config -> config.yaml
        self.config = config["data_ingestion"]
        self.source_path = self.config["source_path"]
        self.filename = self.config["filename"]
        self.train_test_ratio = self.config["train_size"]

        os.makedirs(RAW_DIR, exist_ok=True)

        logger.info(f"Data Ingestion started with {self.filename}")

    def get_data(self):
        try:
            logger.info("Getting Data")

            data = pd.read_csv(self.source_path)
            data.to_csv(RAW_FILE_PATH, index=False)
            logger.info("Data stored in RAW")

        except Exception as e:
            logger.error(f"Error occurred while getting data from {self.source_path}")
            raise CustomException(f"Failed to store data in {RAW_FILE_PATH}")
        
    def split_data(self):
        try:
            logger.info("Starting the splitting process")
            data = pd.read_csv(RAW_FILE_PATH)
            train_data, test_data = train_test_split(data, test_size=1-self.train_test_ratio, random_state=42)

            # both train_data and test_data are in dataframe format but we need in csv format
            train_data.to_csv(TRAIN_FILE_PATH)
            test_data.to_csv(TEST_FILE_PATH)

            logger.info(f"Train data saved to {TRAIN_FILE_PATH}")
            logger.info(f"Test data saved to {TEST_FILE_PATH}")

        except Exception as e:
            logger.error("Error while splitting data")
            raise CustomException("Failed to split the data into training and test sets", e)
    
    def run(self):
        try:
            logger.info("Starting data ingestion process")
            self.get_data()
            self.split_data()

            logger.info("Data Ingestion Completed")
        except CustomException as ce:
            logger.error(f"CustomException : {str(ce)}")

        finally :
            logger.info("Data Ingestion successfully completed")
        
if __name__=="__main__":
    data_ingestion = DataIngestion(read_yaml(CONFIG_PATH))
    data_ingestion.run()