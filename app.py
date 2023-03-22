import pandas as pd
import pymongo
from config import MongoDatabase
from etl import extract, transform

database = MongoDatabase('FraudulentTransactionsProject', 'transactions')

# Extraction and transform process
# transactionsDataExtracted = extract('Fraud.csv')
# transactionsDataTransformed = transform(transactionsDataExtracted)
# print("Data cleaned...")

# Transform dataframe to dict and add index
# transactionsDataTransformed.reset_index(inplace=True)
# transactionsDataTransformed_dict = transactionsDataTransformed.iloc[:1500000].to_dict("records")
# print(transactionsDataTransformed_dict[0])

# Insert all documents in collection
# database.collection.insert_many(transactionsDataTransformed_dict)

# Delete all documents in collection
# database.collection.delete_many({})

database.collection.create_index('isFraud')
database.collection.create_index('amount')
database.collection.create_index('isFlaggedFraud')
database.collection.create_index('type')
exp = database.collection.find({'isFraud': 1}).explain()

# Total sum fraudulent transactions
pipeline = [
    {"$match": {"isFraud": 1}},
    {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
]
result = database.collection.aggregate(pipeline)
print(list(result))

# Avg fraudulent transactions
pipeline = [
    {"$group": {"_id": "$type", "avg_amount": {"$avg": "$amount"}}}
]
result = database.collection.aggregate(pipeline)
print(list(result))

# Transactions number by type
pipeline = [
    {"$match": {"isFraud": 1}},
    {"$group": {"_id": "$type", "count": {"$sum": 1}}}
]
result = database.collection.aggregate(pipeline)
print(list(result))

transacciones_fraudulentas = database.collection.find({"amount": {"$gte": 1000000000}, "isFraud": 1})
for i in transacciones_fraudulentas:
    print(i)