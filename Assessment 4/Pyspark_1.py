# Databricks notebook source
simpleData =(("James","Sales",3000),("Michael", "Sales", 4600),("Robert", "Sales", 4100),("Maria", "Finance", 3000),("James", "Sales", 3000),("Scott", "Finance", 3300), ("Jen", "Finance", 3900), ("Jeff", "Marketing", 3000), ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100))
columns=["employee_name", "department", "salary"]
df =spark.createDataFrame(data=simpleData,schema=columns)


# COMMAND ----------

df.show()

# COMMAND ----------



# COMMAND ----------


simpleData = (("i-101","p110",23, 1),
("i-102", "p111", 50, 1),
("i-101","p110",24,2),
("i-103", "p111", 50, 3), 
("i-104", "p112", 75, 1), 
("i-105", "p114", 125, 1), 
("i-106", "p115", 100, 1), 
("i-107", "p115", 100, 1), 
("i-108", "p114", 125, 2), 
("i-109", "p113", 100, 1), 
("i-110", "p111", 50, 2) ) 

 

columns= ["invoice_no", "product_id", "unit_price", "quantity"] 

 

df = spark.createDataFrame(data = simpleData, schema = columns) 
df.show()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import countDistinct,col
order_df = df.orderBy(col("product_id"),col("quantity"))
order_df.show()
#df2=df.select(countDistinct("quantity"))
#df2.show()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import dense_rank
windowSpec = Window.partitionBy("quantity").orderBy("product_id")
df2=df.withColumn("dense_rank",dense_rank().over(windowSpec)).sort(df.invoice_no.asc()).show()

# COMMAND ----------

df.groupBy("invoice_no").count().show()

# COMMAND ----------

df.groupBy("quantity").count().show()



