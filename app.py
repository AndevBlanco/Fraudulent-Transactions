import pandas as pd
import pymongo
from config import MongoDatabase
from argparse import ArgumentParser
from cleaning import Cleaning
from optimization import Optimization
from queries import Queries
from model import Model
import time


database = MongoDatabase('FraudulentTransactionsProject', 'transactions')
dataset_ref = 'Fraud.csv'

class App:
    def __init__(self, args):
        if "clean" in args and args["clean"]:
            Cleaning(dataset_ref, database)
        
        if "optimize" in args and args["optimize"]:
            # Apply optimization to improve queries performance
            Optimization(database)
            start_time = time.time()

            Queries(database)
            end_time = time.time()
            print("Execution time:", end_time - start_time, "seconds")

        if "queries" in args and args["queries"]:
            start_time = time.time()
            Queries(database)
            end_time = time.time()
            print("Execution time:", end_time - start_time, "seconds")

        if "model" in args and args["model"]:
            Model(database)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-c", "--clean", dest="clean", help="Clean the csv data", required=False, action="store_true")
    parser.add_argument("-o", "--optimize", dest="optimize", help="Optimize database", required=False, action="store_true")
    parser.add_argument("-q", "--queries", dest="queries", help="Execute queries to database", required=False, action="store_true")
    parser.add_argument("-m", "--model", dest="model", help="Run the model and make the prediction", required=False, action="store_true")
    args = vars(parser.parse_args())
    app = App(args)