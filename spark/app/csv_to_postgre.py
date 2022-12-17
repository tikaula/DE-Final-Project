import pandas as pd
import sqlalchemy

#read data
test = pd.read_csv('spark/resources/application_test.csv')
train = pd.read_csv('spark/resources/application_train.csv')

#load to postgre
conn = sqlalchemy.create_engine(url='postgresql://postgres:password@localhost:5432/postgres')

test.to_sql('home_credit_default_risk_application_test', con=conn, index=False, if_exists='replace')
train.to_sql('home_credit_default_risk_application_train', con=conn, index=False, if_exists='replace')