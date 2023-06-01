import pandas as pd
import re

class Cleaning:
    def __init__(self, dataset_ref, database):
        # Delete all documents in collection
        print("Dropping database...")
        database.collection.delete_many({})

        # Extraction and transform process
        self.extract(dataset_ref)
        self.transform()

        # Transform dataframe to dict and add index
        self.dataset.reset_index(inplace=True)
        transactionsDataTransformed_dict = self.dataset.to_dict("records")
        # transactionsDataTransformed_dict = self.dataset.iloc[:1000000].to_dict("records")

        # Insert all documents in collection
        database.collection.insert_many(transactionsDataTransformed_dict)
        print("Data uploaded!")


    # Extract data from the CSV file
    def extract(self, dataset_url):
        transactionsDataExtracted = pd.read_csv(dataset_url)
        self.dataset = transactionsDataExtracted

    # Transform data
    def transform(self):
        data_types = {
            "step": int,
            "type": "string",
            "amount": float,
            "nameOrig": "string",
            "oldbalanceOrg": float,
            "newbalanceOrig": float,
            "nameDest": "string",
            "oldbalanceDest": float,
            "newbalanceDest": float,
            "isFraud": float,
            "isFlaggedFraud": float
            }
        # Check for null values in columns and remove them
        if self.dataset.isnull().values.any():
            print("Some columns contains null values")
            print("Cleaning up...")
            self.dataset = self.dataset.dropna()

        # Remove duplicates
        self.dataset = self.dataset.drop_duplicates()

        # Remove transfer less than 10
        self.dataset = self.dataset[~self.dataset['amount'].astype(str).str.contains(r'^\d{0,1}\.')]

        transactionsDataExtractedNotNumericValues = self.dataset
        # Check that numeric columns have only numeric values
        for i in data_types:
            if data_types[i] != "string":
                if pd.to_numeric(self.dataset[i], errors='coerce').notnull().all() == False:
                    print(f"{i} column contains not integer values")
                    print("Cleaning up...")
                    transactionsDataExtractedNotNumericValues = transactionsDataExtractedNotNumericValues[pd.to_numeric(self.dataset[i], errors='coerce').notnull()]

        self.dataset = transactionsDataExtractedNotNumericValues

