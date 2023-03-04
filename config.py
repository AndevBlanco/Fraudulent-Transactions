import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

class MongoDatabase:
    def __init__(self, database_name, collection_name):
        self.client = MongoClient(os.getenv("DB_URI"))
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]