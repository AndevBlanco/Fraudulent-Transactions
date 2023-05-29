class Queries:
    def __init__(self, database):
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
        # transacciones_fraudulentas = database.collection.find({"amount": {"$gte": 1000000000}, "isFraud": 1})