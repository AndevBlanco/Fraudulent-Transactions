import pandas as pd
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.svm import SVC
from sklearn.metrics import roc_auc_score as ras
from sklearn.metrics import plot_confusion_matrix
import matplotlib.pyplot as plt

class Model:
    def __init__(self, database):
        # Get the data from db and convert to dataframe
        dataframe = pd.DataFrame(list(database.collection.find().limit(100000)))
        print(dataframe.describe())

        # Convert 'type' categorical variable into numerical dummy variables
        type_new = pd.get_dummies(dataframe['type'], drop_first=True)
        # Concatenate the original dataframe with the new dummy variables
        new_dataframe = pd.concat([dataframe, type_new], axis=1)

        # Drop unnecessary columns
        X = new_dataframe.drop(['_id', 'index', 'isFraud', 'type', 'nameOrig', 'nameDest', 'isFlaggedFraud'], axis=1)

        # Define the target variable
        y = new_dataframe['isFraud']

        print(X.shape)
        print(y.shape)

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        # Model 1: logistic regression
        logisticRegressionModel = LogisticRegression(max_iter=1000)

        # Train the Logistic Regression model
        logisticRegressionModel.fit(X_train, y_train)

        # Make predictions on the training set
        train_preds = logisticRegressionModel.predict_proba(X_train)[:, 1]
        print('Training Accuracy Logistic Regression: ', ras(y_train, train_preds))
        
        # Make predictions on the validation set
        y_preds = logisticRegressionModel.predict_proba(X_test)[:, 1]
        print('Validation Accuracy Logistic Regression: ', ras(y_test, y_preds))
        plot_confusion_matrix(logisticRegressionModel, X_test, y_test)


        plt.show()


        