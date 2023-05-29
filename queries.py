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