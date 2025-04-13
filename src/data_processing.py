import os
import pandas as pd
import numpy as np
from src.logger import get_logger
from src.custom_exception import CustomException
from config.paths_config import *
from utils.common_functions import read_yaml, load_data
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler

logger = get_logger(__name__)

class DataProcessor:

    def __init__(self, train_path, test_path, processed_dir, config_path):  # refering to .yaml file
        self.train_path = train_path
        self.test_path = test_path
        self.processed_dir = processed_dir

        self.config = read_yaml(config_path)

        if not os.path.exists(self.processed_dir):
            os.makedirs(self.processed_dir)
        
    def preprocessed_data(self, df):
        try:
            logger.info("Data preprocessing step")

            logger.info("Dropping null entries")
            df.dropna(axis=0, inplace=True)

            logger.info("Dropping the columns")
            df.drop(columns=['Unnamed: 0', 'id', 'City', 'Profession', 'Work Pressure', 'Job Satisfaction', 'Degree'], axis=1, inplace=True)

            cat_cols = self.config["data_processing"]["categorical_columns"]
            num_cols = self.config["data_processing"]["numerical_columns"]


            logger.info("Applying Label Encoding on binary columns")

            # Label encode binary categorical variables

            binary_cols = self.config["data_processing"]["binary_columns"]
            le = LabelEncoder()
            for col in binary_cols:
                df[col] = le.fit_transform(df[col])

            logger.info("Applying OHE")

            # One-hot encode multi-class categoricals with int dtype
            df = pd.get_dummies(df, 
                      columns=self.config["data_processing"]["multivariable_columns"], 
                      drop_first=True, 
                      dtype=int)  # Ensures int dtype instead of bool
            
            logger.info("Applying Standardization")
            
            scaler = StandardScaler()
            for col in num_cols:
                df[col] = scaler.fit_transform(df[col].values.reshape(-1, 1)).ravel()
            
            return df
        except Exception as e:
            logger.error("Error in preprocessing the data")
            raise CustomException("Failed to preprocess the data")
    
    def save_data(self, df, file_path):
        try:
            logger.info("Saving our data in processed folder")
            df.to_csv(file_path, index=False)
            logger.info(f"Data saved successfully to {file_path}")

        except Exception as e:
            logger.error(f"Error during saving data step {e}")
            raise CustomException("Error while saving data")
        
    
    
    def process(self):

        try:
            logger.info("Loading data from RAW directoy")

            train_df = load_data(self.train_path)
            test_df = load_data(self.test_path)

            train_df = self.preprocessed_data(train_df)
            test_df = self.preprocessed_data(test_df)

            self.save_data(train_df, PROCESSED_TRAIN_DATA_PATH)
            self.save_data(test_df, PROCESSED_TEST_DATA_PATH)

            logger.info("Data Processing completed successfully")
        
        except Exception as e:
            logger.error(f"Error during preprocessing pipeline", e)
            raise CustomException("Error in data preprocessing pipeline")

if __name__=="__main__":

    processor = DataProcessor(TRAIN_FILE_PATH, TEST_FILE_PATH, PROCESSED_DIR, CONFIG_PATH)
    processor.process()
    
