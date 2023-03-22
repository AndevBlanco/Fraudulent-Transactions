import pandas as pd
import json

# Extract data from the CSV file
def extract(dataset_url):
    transactionsDataExtracted = pd.read_csv(dataset_url)
    
    return transactionsDataExtracted 
    
# Transform data
def transform(dataset):
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
    # Check for null values in step column
    if dataset.isnull().values.any():
        print("Some columns contains null values")
        print("Cleaning up...")
        dataset = dataset.dropna()

    transactionsDataExtractedNotNumericValues = dataset
    # Check for just integer values in step column
    for i in data_types:
        if data_types[i] != "string":
            if pd.to_numeric(dataset[i], errors='coerce').notnull().all() == False:
                print(f"{i} column contains not integer values")
                print("Cleaning up...")
                transactionsDataExtractedNotNumericValues = transactionsDataExtractedNotNumericValues[pd.to_numeric(dataset[i], errors='coerce').notnull()]

    return transactionsDataExtractedNotNumericValues
