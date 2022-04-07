#!/usr/bin/env python
# coding: utf-8

# In[15]:


pip install beautifulsoup4


# In[12]:


import requests

a="https://api.covidtracking.com/v1/us/daily.json "

r=requests.get(a).json()

print(r)


# In[13]:


import requests
import pandas as ms
import json


# In[14]:


a=requests.get("https://api.covidtracking.com/v1/us/daily.json")


# In[15]:


r=json.loads(a.text)
print(r)


# In[16]:


df=a.json()
df1=ms.DataFrame(df)
df1


# In[17]:


df1.columns


# In[18]:


df1.dtypes


# In[19]:


import pandas as pd
from sqlalchemy import create_engine
create=create_engine("mysql+pymysql://root:Shri1525@localhost/retail")


# In[21]:


pip install sqlalchemy


# In[22]:


df1.to_csv('json_file.csv')


# In[26]:


df5=pd.read_csv('json_file.csv',index_col=False)
df5.to_sql('json_filet',create,if_exists='replace',index=False)


# In[27]:


pip install ipython


# In[28]:


import pandas as pd
from sqlalchemy import create_engine
create=create_engine("mysql+pymysql://root:Shri1525@localhost/retail")


# In[29]:


df5=pd.read_csv('json_file.csv',index_col=False)
df5.to_sql('json_filet',create,if_exists='replace',index=False)


# In[30]:


df1=pd.read_sql_table('json_filet',create)
df1


# In[7]:


df1['positive'].value_counts()


# In[9]:


df2=pd.DataFrame(df1.groupby(['date'])['positive'].count().reset_index())
df2


# In[12]:


df1['hospitalizedCurrently'].value_counts()


# In[13]:


df2=pd.DataFrame(df1.groupby(['date'])['hospitalizedCurrently'].count().reset_index())
df2

