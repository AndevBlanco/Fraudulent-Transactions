class Optimization:
    def __init__(self, database):
        database.collection.create_index('isFraud')
        database.collection.create_index('amount')
        database.collection.create_index('isFlaggedFraud')
        database.collection.create_index('type')
        exp = database.collection.find({'isFraud': 1}).explain()