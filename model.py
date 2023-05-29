import pandas as pd
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.metrics import roc_auc_score as ras

class Model:
    def __init__(self, database):
        # Get the data from db and convert to dataframe
        dataframe = pd.DataFrame(list(database.collection.find().limit(1000)))
        print(dataframe.describe())

        # sns.countplot(x='type', data=dataframe)
        type_new = pd.get_dummies(dataframe['type'], drop_first=True)
        new_dataframe = pd.concat([dataframe, type_new], axis=1)
        # sns.barplot(x='type', y='amount', data=dataframe)
        X = new_dataframe.drop(['_id', 'index', 'isFraud', 'type', 'nameOrig', 'nameDest', 'isFlaggedFraud'], axis=1)
        y = new_dataframe['isFraud']
        print(X.shape)
        print(y.shape)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        logisticRegressionModel = LogisticRegression()
        logisticRegressionModel.fit(X_train, y_train)

        train_preds = logisticRegressionModel.predict_proba(X_train)[:, 1]
        print('Training Accuracy Logistic Regression: ', ras(y_train, train_preds))
        y_preds = logisticRegressionModel.predict_proba(X_test)[:, 1]
        print('Validation Accuracy Logistic Regression: ', ras(y_test, y_preds))

        