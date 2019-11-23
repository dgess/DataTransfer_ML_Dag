# A simple Airflow DAG that will (on a daily basis at 10:00 am on the server):
# 1) Take data in MongoDB to a Pandas DataFrame.
# 2) Train a ML model for a Flask application.
# 3) Take data from Pandas DataFrame and import into MySQL.
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow import DAG
import pandas as pd

default_args = {
        'owner' : 'dgessler',
        'retries' : 1,
        'retry_delay' : timedelta(minutes = 5),
        'email_on_failure' : False,
        'email_on_retry' : True,
        'start_date' : datetime.datetime(2019,11,22)
        }

# We define the DAG...
dag = DAG('MongoDB_To_MySQL', default_args = default_args, schedule_interval = '* 10 * * *', catchup = False)

# We now export the data from MongoDB to a DataFrame...
def get_mongo_data():
    
    # This function will parse the dictionary and keys of the documents to lists for the DataFrame.
    def to_df():
        
        # We connect to the Mongo Database...
        from pymongo import MongoClient
        client = MongoClient(port = 27017)
        db = client.airflow_dag

        # While we could use the JSON package, we use a For Loop instead...
        # I am unsure if the JSON Python package will be able to parse nested dictionaries...
        # If so, this for loop can be expanded depending on the Data...
        columns = []
        data = []
        for doc in db.airflow_test.find():
            values = []
            for key,val in doc.items():
                if key not in columns:
                    columns.append(key)
                    values.append(val)
                    data.append(values)
                    
        # We pass the column names and the column data to a DataFrame...
        return pd.DataFrame(data = data, columns = columns)
    
    return to_df()

get_mongo = PythonOperator(
        dag_id = 'get_mongo',
        python_callable = get_mongo_data,
        dag = dag
        )

# With the data now in a DataFrame... we create an algorithim for the data.
def ml():
    
    def train():
        # We get our data from the DataFrame...
        df = get_mongo_data()
        
        # We train a KNN model...
        from sklearn.model_selection import train_test_split
        from sklearn.neighbors import KNeighborsClassifier
        from sklearn.metrics import accuracy_score
        
        X = df.drop(columns = ['species', '_id'])
        y = df[['species']]
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 23)
        
        # The following will train the model up to 10 nearest-neighbors...
        vals = []
        for rad in enumerate(range(1,10)):
            neigh = KNeighborsClassifier(n_neighbors = rad)
            model = neigh.fit(X_train, y_train) 
            
            pred = model.predict(X_test)
    
            vals.append({rad[0] : accuracy_score(y_test, pred)})

        # While it is understood better accuracy measures must occur for selecting a model,
        # for demonstrative purposes, we select the first KNN model that satisfies our
        # accuracy measurements...
        model = None
        for index in range(len(vals)):
            for key in vals[index]:
                if vals[index][key] >= 0.95 and vals[index][key] < 0.98:
                    model = key + 1
                    break
                
        # If a model was selected, we pickle the model for a Flask application that pulls from this model...
        if model > 0:
            import pickle
            out = open('\srv\www\flask_app\predict.py', 'wb')
            pickle.dump(KNeighborsClassifier(n_neighbors = model).fit(X_train, y_train) , out)
            out.close()
            
            return print('Model succesfully updated...')
        
        # Else, we return to sending the DataFrame to MuSQL.
        else:
            return print('No model selected... using previous learned model.')
        
    
train_model = PythonOperator(
        task_id = 'train_model',
        python_callable = ml,
        dag = dag
        )

# We now import the DataFrame to  MySQL.
def to_mysql():
    from sqlalchemy import create_engine
    import pymysql
    
    # We establish our SQLAlchemy connection...
    eng = create_engine('mysql+pymysql://mysql_user:pass@123.45.6.789/daily_processing_db')
    
    # We return our DataFrame...
    df = get_mongo_data()
    
    # We import the DataFrame into the MySQL database as chunks as we do not know the RAM that the server has available...
    df.to_sql(name = 'Species_Data', con = eng, chunksize = df.shape[0]/10, if_exists = 'replace', index = False)
    
# We now define the Airflow task...
to_mysql = PythonOperator(
        task_id = 'to_mysql',
        python_callable = to_mysql,
        dag = dag
        )

# We now sequence the tasks...
get_mongo << train_model << to_mysql

# We call it a DAG.
    
    
    
    
    
    
    
        
