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

        # 100 Largest fraudulent transactions
        pipeline = [
            {"$match": {"isFraud": 1}},
            { "$sort": {"amount": -1 } },  # Ordenar los documentos por el campo 'amount' de forma descendente
            { "$limit": 100 },             # Limitar los resultados a los primeros 100 documentos
            { "$project": { "_id": 0, "amount": 1, "nameOrig": 1 } }  # Proyectar solo los campos 'amount' y 'nameOrig' en el resultado
        ]
        result = database.collection.aggregate(pipeline)
        print("Los 100 mas grandes: ")
        print(list(result))

        # Group by amount
        pipeline = [
            {"$match": {"isFraud": 1}},
            {"$group": {"_id": "$amount", "count": {"$sum": 1}}},
            { "$sort": {"_id": -1 } },
            { "$limit": 10 }
        ]
        result = database.collection.aggregate(pipeline)
        print("Gruop by amount 10 results from less to high: ")
        print(list(result))

        # Max and Min
        pipeline = [
            {"$match": {"isFraud": 1}},
            {"$group": {"_id": None, "maxAmount": { "$max": "$amount"}}}
        ]
        print("Maximun: ")
        result = database.collection.aggregate(pipeline)
        print(list(result))

        pipeline = [
            {"$match": {"isFraud": 1}},
            {"$group": {"_id": None, "minAmount": { "$min": "$amount"}}}
        ]
        print("Minimum: ")
        result = database.collection.aggregate(pipeline)
        print(list(result))