import pandas as pd
import numpy as np
from scipy import stats
from config import MongoDatabase

database = MongoDatabase('FraudulentTransactionsProject', 'transactions')

print(database.collection.find())
for i in database.collection.find():
    print(i)

# Extract data from the CSV file
df = pd.read_csv('Fraud.csv')


# Transform data
df = df.dropna()

# df[(np.abs(stats.zscore(df)) < 3).all(axis=1)]


print(df)