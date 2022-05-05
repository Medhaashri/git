#!/usr/bin/env python
# coding: utf-8

# In[ ]:


key=AIzaSyBEKUSeI6-LWIosnQJlXs-oWS09WeovowQ

url=https://www.googleapis.com/youtube/v3/channels?id=7lCDEYXw3mM&key=AIzaSyBEKUSeI6-LWIosnQJlXs-oWS09WeovowQ&part=snippet,contentDetails,statistics,status
# In[ ]:


channel=UCeqCpzE_Au5BQ6bJIiWRLjA/featured


# In[ ]:


vdo=QBSKnVuk-po


# In[14]:


pip install requests


# In[18]:


url="https://www.googleapis.com/youtube/v3/channels?id=UCeqCpzE_Au5BQ6bJIiWRLjA&key=AIzaSyBEKUSeI6-LWIosnQJlXs-oWS09WeovowQ&part=snippet,contentDetails,statistics,statistics,status"


# In[19]:


import pandas as pd


# In[20]:


import requests
r=requests.get(url).json()
r


# In[21]:


df=pd.json_normalize(r,max_level=1)
df


# In[22]:



df1= pd.json_normalize(r["items"]) 
df1


# In[26]:


newdf=df1[["id","snippet.title","snippet.description","snippet.publishedAt","contentDetails.relatedPlaylists.uploads","statistics.viewCount","statistics.subscriberCount","statistics.hiddenSubscriberCount","statistics.videoCount"]]
newdf


# In[28]:


df3=newdf.to_csv("youtube_project.csv")
df3


# In[30]:


from sqlalchemy import create_engine
import pymysql
create=create_engine("mysql+pymysql://root:Shri1525@localhost/project")
newdf.to_sql('YTUBE_CHANNELS',create,if_exists='replace',index=False)


# In[34]:


URL="https://www.googleapis.com/youtube/v3/videos?id=caWrxvC6TSk&key=AIzaSyBEKUSeI6-LWIosnQJlXs-oWS09WeovowQ&part=snippet,contentDetails,statistics,status"


# In[35]:


import pandas as pd
import requests
r=requests.get(URL).json()
r


# In[42]:


df=pd.json_normalize(r,max_level=1)
df


# In[43]:


df= pd.json_normalize(r["items"]) 
df


# In[44]:


newdf=df[["id","snippet.title","snippet.description","snippet.publishedAt","statistics.favoriteCount","statistics.commentCount","snippet.categoryId","snippet.channelId","snippet.channelTitle","contentDetails.duration"]]
newdf


# In[45]:


df2=newdf.to_csv("video.csv")
df2


# In[46]:


from sqlalchemy import create_engine
import pymysql
create=create_engine("mysql+pymysql://root:Shri1525@localhost/project")
newdf.to_sql('YTUBE_VIDEOS',create,if_exists='replace',index=False)


# In[ ]:




