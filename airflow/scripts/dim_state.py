import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:password@localhost:5432/postgres")

#load data 
companies = pd.read_sql(f"select * from companies", con=engine)
dim_country = pd.read_sql(f"select * from dim_country", con=engine)

# set dim_state data
cols = ['state_code']
fill_empty_state = companies['state_code'] != ''
dim_state = companies[fill_empty_state][cols].groupby(cols).count().reset_index().reset_index()
dim_state = dim_state.rename(columns={"index":"state_id"})
dim_state['state_id'] = dim_state['state_id'] + 1 
dim_state = dim_state.merge(dim_country, on='state_code').drop(columns=['country_code'])

#load to database
dim_state.to_sql('dim_state', con=engine, index=False, if_exists='replace')

