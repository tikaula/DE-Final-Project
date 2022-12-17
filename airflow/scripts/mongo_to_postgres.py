import pymongo
import pandas as pd
import numpy as np
import sqlalchemy

client = pymongo.MongoClient('mongodb+srv://tikaulla:ayamgoreng@cluster1.lqdkxk9.mongodb.net/?retryWrites=true&w=majority')

## Read Data from collection in mongodb
collection = client['sample_training']
collection_zips = collection['zips']
collection_companies = collection['companies']

## ZIPS
### Membuat Zips Dataframe
zips_cursor = collection_zips.find({}, {'_id':0})
zips=pd.DataFrame.from_dict(list(zips_cursor))

### Flatten loc column to x=long & y=lat
loc_list = pd.DataFrame(zips['loc'].tolist())              # membuat dataframe location
zips = pd.concat([zips, loc_list], axis=1)                 # menggabungkan 2 dataframe
zips.drop(['loc'], axis=1, inplace=True)                   # menghapus kolom loc karena sudah tergantingan oleh kolom x dan kolom y
zips.rename(columns={'x':'long', 'y':'lat'}, inplace=True) # rename kolom x dan y

## COMPANIES
### Exclude all nested column
exc_cols = ['_id','offices','image','products','relationships','competitions','screenshots','providerships','funding_rounds','investments',
'acquisition','acquisitions','milestones','video_embeds','screenshoots','external_links','partners','ipo']

### Get only first object in offices
comp = collection_companies.aggregate([
    {'$addFields': {'office':{'$first':'$offices'}}},
    {'$unset' : exc_cols}], allowDiskUse=True)

###Membuat Companies DataFrame
companies = pd.DataFrame.from_dict(list(comp))

### fill empty office value with default empty office dict
null_comp = {
    'description': '',
    'address1':'',
    'address2':'',
    'zip_code':'',
    'city':'',
    'state_code':'',
    'country_code':'',
    'latitude':None,
    'latitude':None
}
companies['office']=np.where(companies['office'].notna(), companies['office'], null_comp)

### Falatten office column
ofc_list = pd.DataFrame(companies['office'].tolist())      # membuat dataframe office
companies = pd.concat([companies, ofc_list], axis=1)       # menggabungkan 2 dataframe
companies.drop(['office'], axis=1, inplace=True)           # drop kolom office sebelum flatten

## Load to Postgresql
conn = sqlalchemy.create_engine(url='postgresql://postgres:password@localhost:5432/postgres')

zips.to_sql('zips', con=conn, index=False, if_exists='replace')
companies.to_sql('companies', con=conn, index=False, if_exists='replace')



