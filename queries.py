class Queries:
    def __init__(self, database):
        # Total sum fraudulent transactions
        total = database.collection.aggregate([
            {"$match": {"isFraud": 1}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ])
        result = list(total)
        print('Total fraudulent transactions: ', result[0]['total'])

        # Total sum fraudulent transactions by groups
        total_sum = database.collection.aggregate([
            { "$match": { "isFraud": 1 } },
            { "$group": { "_id": "$type", "totalAmount": { "$sum": "$amount" }, "count": { "$sum": 1 } } }
        ])
        total_sum_result = list(total_sum)
        print('\nTotal fraudulent transactions by groups:')
        for i in total_sum_result:
            print('  ', i['_id'] + ': ', round(i['totalAmount'], 2))

        # Avg amount fraudulent transactions
        avg = database.collection.aggregate([
            { "$match": { "isFraud": 1 } },
            {"$group": {"_id": "$type", "avg_amount": {"$avg": "$amount"}}}
        ])
        result_avg = list(avg)
        print('\nAverage amount fraudulent transactions by type:')
        for i in result_avg:
            print('  ', i['_id'] + ': ', round(i['avg_amount'], 2))

        # Number of fraudulent transactions by type
        number_by_type = database.collection.aggregate([
            {"$match": {"isFraud": 1}},
            {"$group": {"_id": "$type", "count": {"$sum": 1}}}
        ])
        result_number_by_type = list(number_by_type)
        print('\nTransactions number by type:') 
        for i in result_number_by_type:
            print('  ', i['_id'] + ': ', i['count'])
      
        # 100 Largest fraudulent transactions
        pipeline = [
            {"$match": {"isFraud": 1}},
            { "$sort": {"amount": -1 } },                              
            { "$limit": 100 },                                         
            { "$project": { "_id": 0, "amount": 1, "nameOrig": 1 } } 
        ]
        result = database.collection.aggregate(pipeline)
        print("100 Largest fraudulent transactions: ")
        print(list(result))

        # Group by biggest amount
        pipeline = [
            {"$match": {"isFraud": 1}},
            {"$group": {"_id": "$amount", "count": {"$sum": 1}}},
            { "$sort": {"_id": -1 } },
            { "$limit": 10 }
        ]
        result = database.collection.aggregate(pipeline)
        print("From high to less: ")
        print(list(result))

        #Maximun and minimun
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
        print("Minimun: ")
        result = database.collection.aggregate(pipeline)
        print(list(result))


        amount_gte = database.collection.find({"amount": {"$gte": 10000000}, "isFraud": 1})
        result_amount_gte = list(amount_gte)
        print(f'\nTransactions amount greater than 10.000.000: {len(result_amount_gte)}') 


        # Get most frequent beneficiaries in fraudulent transactions
        beneficiaries = database.collection.aggregate([
            {"$match": {"isFraud": 1}},
            {"$group": {"_id": "$nameDest", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ])
        beneficiaries_result = list(beneficiaries)
        print('\nBeneficiaries:')
        for i in beneficiaries_result:
            print('  ', i['_id'] + ': ', i['count'])

