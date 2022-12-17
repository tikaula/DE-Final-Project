import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:password@localhost:5432/postgres")

#load data 
companies = pd.read_sql(f"select * from companies", con=engine)

# set dim_country data
cols = ['country_code','state_code']
fill_empty_country = companies['country_code'] != ''
dim_country = companies[fill_empty_country][cols].groupby(cols).count().reset_index().reset_index()
dim_country = dim_country.rename(columns={"index":"country_id"})
dim_country['country_id'] = dim_country['country_id'] + 1 

#load to database
dim_country.to_sql('dim_country', con=engine, index=False, if_exists='replace')
