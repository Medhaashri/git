#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# In[ ]:


url="https://en.wikipedia.org/wiki/List_of_prime_ministers_of_India"
table_class="wikitable sortable jquery-tablesorter"
r=requests.get(url)
s=BeautifulSoup(r.text,'html.parser')
print(s)


# In[ ]:


i=s.find('table',{'class':"wikitable"})
print(i)


# In[ ]:


df=pd.read_html(str(i))
df=pd.DataFrame(df[0])
df.columns =['No','No.1','N0','Name','Party','Took Office','Left Office','lig','thing','twenty','king','queen']
df


# In[ ]:


df2=df[['Name','Party','Took Office','Left Office']]
print(df2)


# In[171]:


df2['Took Office']=df2['Took Office'].str.replace('[[§]]','')


# In[172]:


df2['Took Office']=df2['Took Office'].str.replace('[','')


# In[173]:


df2['Left Office']=df2['Left Office'].str.replace('[NC]','')


# In[174]:


df2['Left Office']=df2['Left Office'].str.replace('[RES]','')


# In[175]:


df2['Left Office']=df2['Left Office'].str.replace('[[]','')


# In[176]:


df2['Left Office']=df2['Left Office'].str.replace('[]]','')


# In[177]:


df2['Left Office']=df2['Left Office'].str.replace('[†]','')


# In[178]:


df2.drop(26)


# In[179]:


df2["Name"]=df2["Name"].str.split('(').str[0]


# In[180]:


df2.loc[[15],"Left Office"]='10 November 1990'


# In[181]:


df2.drop(26)


# In[182]:


df2['Took Office'] = pd.to_datetime(df2['Took Office'], infer_datetime_format=True)
print(df2)


# In[183]:


df2=df2.drop(26)
print(df2)


# In[184]:


df2.loc[[15],"Left_Office"]='10 November 1990'


# In[185]:


df2['Left Office'] = pd.to_datetime(df2['Left Office'], infer_datetime_format=True)
print(df2)


# In[186]:

create=create_engine("mysql+pymysql://root:Shri1525@localhost/retail")


# In[ ]:


# In[ ]:


df2.to_csv('pm_india.csv')


# In[ ]:


df5=pd.read_csv('pm_india.csv',index_col=False)
df2.to_sql('pm_india',create,if_exists='replace',index=False)


# In[ ]:





# In[ ]:




