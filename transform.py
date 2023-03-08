import pandas as pd

class TransformDataTransactions:
    def checkAndCleanNullValues(self, transactionsDataExtracted, column):
        if transactionsDataExtracted[column].isnull().values.any():
            print(f"{column} column contains null values")
            print("Cleaning up...")
            transactionsDataExtracted = transactionsDataExtracted[column].dropna()

    def checkAndCleanNotNumericValues(self, transactionsDataExtracted, column):
        if pd.to_numeric(transactionsDataExtracted[column], errors='coerce').notnull().all() == False:
            print(f"{column} column contains not integer values")
            print("Cleaning up...")
            transactionsDataExtracted[transactionsDataExtracted[column].apply(lambda x: x.isnumeric())]
        