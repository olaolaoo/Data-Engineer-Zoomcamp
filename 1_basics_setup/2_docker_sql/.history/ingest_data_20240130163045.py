#!/usr/bin/env python
# coding: utf-8

# # set up library
# This file was tested with MacOS using Conda for Python management.
# 
# Make sure that your Python env has `pandas` and `sqlalchemy` installed. I also had to install `psycopg2` manually.

# In[6]:


import pandas as pd
import pyarrow.parquet as pq
import os


# In[2]:


pd.__version__


# # download data

# In[7]:


os.system(f"wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet -O yellow_tripdata_2021-01.parquet")


# # load data
# Parquet is the industry standard for working with big data. Using Parquet format results in reduced file sizes and increased speeds. 

# In[8]:


df= pq.read_table("yellow_tripdata_2021-01.parquet").to_pandas()


# In[9]:


df.head()


# # create the ***schema*** for the database
# We will now create the ***schema*** for the database. 
# 
# The _schema_ is the structure of the database; 
# 
# in this case it describes the columns of our table. Pandas can output the SQL ***DDL*** (Data definition language) instructions necessary to create the schema.

# In[10]:


# We need to provide a name for the table; we will use 'yellow_taxi_data'
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))


# # Start:connect to database and create the table
# Even though we have the DDL instructions, we still need specific instructions for Postgres to connect to it and create the table. We will use `sqlalchemy` for this.

# ## step1-create an engine and put schema to postgres
# An ***engine*** specifies the database details in a URI. The structure of the URI is:
# 
# `database://user:password@host:port/database_name`

# In[11]:


from sqlalchemy import create_engine


# In[15]:


# （：//用户名：密码@主机名：端口/数据库名称）
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[16]:


engine.connect()


# In[17]:


# we can now use our engine to get the specific output for Postgres
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# ## step2-load data columns
# We will now finally create the table in the database. With `df.head(n=0)` we can get the name of the columns only, without any additional data. We will use it to generate a SQL instruction to generate the table.

# In[19]:


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

# In[20]:


get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')")


# ## step5-check out data
# Back on the terminal running `pgcli`, we can check how many lines were to the database with:
# 
# ```sql
# SELECT count(1) FROM yellow_taxi_data;
# ```
# 
# You should see 1369769 lines.
