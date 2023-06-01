class Optimization:
    def __init__(self, database):
        database.collection.create_index('isFraud')
        database.collection.create_index('amount')
        database.collection.create_index('isFlaggedFraud')
        database.collection.create_index('type')
        result = database.collection.find({'isFraud': 1}).explain()
        execution_stats = result['executionStats']
        winning_plan = result['queryPlanner']['winningPlan']
        index_used = result['queryPlanner'] \
        .get('winningPlan', {}).get('inputStage', {}).get('indexName')
        print("Execution statistics:", execution_stats)
        print("Winning execution plan:", winning_plan)
        print("Used index:", index_used)