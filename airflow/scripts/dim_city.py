import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:password@localhost:5432/postgres")

#load data 
zips = pd.read_sql(f"select * from zips", con=engine)
dim_state = pd.read_sql(f"select * from dim_state", con=engine)

# set dim_city data
cols = ['state','city', 'zip']
dim_city = zips[cols].groupby(cols).count().reset_index()
dim_city = dim_city.merge(dim_state, left_on='state', right_on='state_code').drop(columns=['state_code','state_id','country_id'])
dim_city = dim_city.groupby(['state', 'city', 'zip']).count().reset_index().reset_index()
dim_city = dim_city[(dim_city.city != '') | (dim_city.zip != '')]
dim_city = dim_city.rename(columns={"index":"city_id"})
dim_city['city_id'] = dim_city['city_id'] + 1

#load to database
dim_city.to_sql('dim_city', con=engine, index=False, if_exists='replace')
