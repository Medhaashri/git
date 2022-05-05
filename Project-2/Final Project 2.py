#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Packages Imports

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode,split
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as _sum


# In[3]:


# Reading JSON converting to DataFrame


# In[ ]:


# Reading JSON converting to DataFrame
df_user = spark.read.json("/FileStore/tables/user/user.json")
df_user.printSchema()
df_user.show()


# In[ ]:


df_business = spark.read.json("/FileStore/tables/ss/yelp_academic_dataset_business.json")
df_business.printSchema()
df_business.show()


# In[4]:


df_business = df_business.withColumn("categories", explode(split("categories", ", ")))


# In[5]:


df_user.printSchema()


# In[6]:


# Selecting only required Columns

df_business = df_business.select("business_id","name","attributes","categories","city","review_count","stars")
df_review = df_review.select("business_id","user_id","stars")
df_user = df_user.select("user_id","name","review_count")

# Renaming Column

df_business = df_business.withColumnRenamed('stars',"business_stars")
df_review = df_review.withColumnRenamed('stars',"review_stars")

df_business = df_business.withColumnRenamed('name',"business_name")
df_user = df_user.withColumnRenamed('name',"user_name")

df_business = df_business.withColumnRenamed('review_count',"business_review_count")
df_user = df_user.withColumnRenamed('review_count',"user_review_count")


# In[7]:


# Inner Join

df_business_join_review = df_business.join(df_review, on=['business_id'], how='inner')
df_business_user_review = df_user.join(df_business_join_review, on=['user_id'], how='inner')


df_business_user_review.show()


# In[8]:


# List the categories and its record count using spark
df_business_user_review.select("categories").groupBy("categories").count().show()


# In[9]:


# Select the Top 5 best rated

df_business_user_review.select("business_name","review_stars").distinct().sort(col("review_stars").desc(),col("business_name").asc()).show(5)


# In[10]:


# Select worst 5 Home Services using spark
df_business_user_review.select("business_name","categories","review_stars").distinct().filter(df_business_user_review.categories == "Home Services").sort(col("review_stars").asc(),col("business_name").desc()).show(5)


# In[11]:


# Select the City which has max high rated Nightlife



business_nightLife = df_business_user_review.select("city","review_stars").filter(df_business_user_review.categories == "Nightlife")
business_nightLife_group_city = business_nightLife.groupBy("city").agg(_sum(col("review_stars")).alias("Sum_Reviews"))
business_nightLife_sort_reviews = business_nightLife_group_city.sort(col("Sum_Reviews").desc())

business_nightLife_sort_reviews.show(1)


# In[22]:


# Get the userid, name, category and review count of all the users and write into CSV
csv = df_business_user_review.select("user_id","user_name","categories","business_review_count","user_review_count")
csv.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true")



