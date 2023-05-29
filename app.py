import pandas as pd
import pymongo
from config import MongoDatabase
from argparse import ArgumentParser
from cleaning import Cleaning
from optimization import Optimization
from queries import Queries
from model import Model

database = MongoDatabase('FraudulentTransactionsProject', 'transactions')
dataset_ref = 'Fraud.csv'

class App:
    def __init__(self, args):
        if "clean" in args and args["clean"]:
            Cleaning(dataset_ref, database)
        
        if "optimize" in args and args["optimize"]:
            Optimization(database)

        if "queries" in args and args["queries"]:
            Queries(database)

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