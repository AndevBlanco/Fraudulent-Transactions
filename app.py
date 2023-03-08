import pandas as pd
import numpy as np
from scipy import stats
from config import MongoDatabase

# database = MongoDatabase('FraudulentTransactionsProject', 'transactions')

# Extract data from the CSV file
transactionsDataExtracted = pd.read_csv('Fraud_test.csv')
dtype = {
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

# Transform data
# Check for null values in step column
if transactionsDataExtracted.isnull().values.any():
    print("Some columns contains null values")
    print("Cleaning up...")
    transactionsDataExtracted = transactionsDataExtracted.dropna()

# Check for just integer values in step column
def checkAndCleanNotNumericValues(column):
    transactionsDataExtractedNotNumericValues = transactionsDataExtracted
    if pd.to_numeric(transactionsDataExtracted[column], errors='coerce').notnull().all() == False:
        print(f"{column} column contains not integer values")
        print("Cleaning up...")
        transactionsDataExtractedNotNumericValues = transactionsDataExtracted[pd.to_numeric(transactionsDataExtracted[column], errors='coerce').notnull()]

    return transactionsDataExtractedNotNumericValues


for i in dtype:
    if dtype[i] != "string":
        transactionsDataExtracted = checkAndCleanNotNumericValues(i)

print(transactionsDataExtracted)


# df[(np.abs(stats.zscore(df)) < 3).all(axis=1)]