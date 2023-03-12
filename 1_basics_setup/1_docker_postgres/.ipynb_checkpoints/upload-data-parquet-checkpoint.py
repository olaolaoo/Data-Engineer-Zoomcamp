#!/usr/bin/env python
# coding: utf-8

# # 注意：⭐️与upload-data-csv文档不同之处在于，csv的文档分批倒入database

# # set up library
# This file was tested with MacOS using Conda for Python management.
# 
# Make sure that your Python env has `pandas` and `sqlalchemy` installed. I also had to install `psycopg2` manually.

# In[1]:


#适用于csv
#df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)
#df


# In[23]:


import pandas as pd
pd.__version__


# # load data
# Parquet is the industry standard for working with big data. Using Parquet format results in reduced file sizes and increased speeds. 

# In[22]:


import pyarrow.parquet as pq


# In[24]:


trips = pq.read_table('yellow_tripdata_2021-01.parquet')
df = trips.to_pandas()


# In[7]:


df.head()


# # create the ***schema*** for the database
# We will now create the ***schema*** for the database. The _schema_ is the structure of the database; in this case it describes the columns of our table. Pandas can output the SQL ***DDL*** (Data definition language) instructions necessary to create the schema.

# In[8]:


# We need to provide a name for the table; we will use 'yellow_taxi_data'
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))


# Note that this only outputs the instructions, it hasn't actually created the table in the database yet.

# Note that `tpep_pickup_datetime` and `tpep_dropoff_datetime` are text fields even though they should be timestamps. Let's change that.

# In[13]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))


# # Start:connect to database and create the table
# Even though we have the DDL instructions, we still need specific instructions for Postgres to connect to it and create the table. We will use `sqlalchemy` for this.

# ## step1-create an engine
# An ***engine*** specifies the database details in a URI. The structure of the URI is:
# 
# `database://user:password@host:port/database_name`

# In[9]:


from sqlalchemy import create_engine


# In[10]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[11]:


# run this cell when the Postgres Docker container is running
engine.connect()


# In[14]:


# we can now use our engine to get the specific output for Postgres
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# ## step2-load data columns
# We will now finally create the table in the database. With `df.head(n=0)` we can get the name of the columns only, without any additional data. We will use it to generate a SQL instruction to generate the table.

# In[ ]:


# we need to provide the table name, the connection and what to do if the table already exists
# we choose to replace everything in case you had already created something by accident before.
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# ## step3-check out new database
# You can now use `pgcli -h localhost -p 5432 -u root -d ny_taxi` on a separate terminal to look at the database:
# 
# * `\dt` for looking at available tables.
# * `\d yellow_taxi_data` for describing the new table.

# ## step4-load all the data
# Let's include our current chunk to our database and time how long it takes.

# In[16]:


get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')")


# ## step5-check out data
# Back on the terminal running `pgcli`, we can check how many lines were to the database with:
# 
# ```sql
# SELECT count(1) FROM yellow_taxi_data;
# ```
# 
# You should see 100,000 lines.
# 

# Let's write a loop to write all chunks to the database. Use the terminal with `pgcli` to check the database after the code finishes running.

# And that's it! Feel free to go back to the [notes](../notes/1_intro.md#inserting-data-to-postgres-with-python)
