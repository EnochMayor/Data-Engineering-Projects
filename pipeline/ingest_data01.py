#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

#pd.__file__

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'



url = f"{prefix} + yellow_tripdata_2021-01.csv.gz"

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    nrows=1000

)


# In[74]:


# Read a sample of the data
#prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
#df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz') #, nrows=100)

# Display first rows
df.head()


# In[27]:


# Check data types
df.dtypes


# In[16]:


# Check data shape
df.shape


# In[17]:


df.info()


# In[18]:


print(df.isnull().sum())


# In[21]:


df['VendorID']


# In[20]:


pd.to_numeric(df['VendorID'], errors='coerce')


# In[28]:


#get_ipython().system('uv add sqlalchemy psycopg2-binary')


# In[75]:



# In[76]:


df.head(0).to_sql





def run(
    year = 2021,
    month = 1,
    chunksize = 100000,

    pg_pass = 'root',
    pg_host = 'localhost',
    pg_user = 'root',
    pg_db = 'ny_taxi',
    target_table = 'yellow_taxi_data',
    pg_port = 5432
):
    print(year, month)

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

    df_iter = pd.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
    
    
            first = False
    
#        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')


        for df_chunk in tqdm(df_iter):
            df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
            print('Inserted;', len(df_chunk))


if __name__=='__main__':
    run()
# In[88]:





# In[ ]:




