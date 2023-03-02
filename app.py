from dotenv import load_dotenv
import os
from pymongo import MongoClient
import pandas as pd

load_dotenv()

client = MongoClient(os.getenv("DB_URI"))

# Extract data from the CSV file
df = pd.read_csv('Fraud.csv')


# Transform data
df = df.dropna()


print(df)