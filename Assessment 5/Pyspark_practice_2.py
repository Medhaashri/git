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

from pyspark.sql.functions import countDistinct,col,concat
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

# COMMAND ----------

from pyspark.sql.functions import sum as sum_of_col
from pyspark.sql.functions import *
simpleData = (("i-101","85123A","ABC",150,6,"2021-12-01 08:16:00","c-1001"), 
("i-102","85124A","XYZ",110,6,"2021-12-01 09:12:00","c-1002"), 
("i-103","85125A","MNO",100,4,"2021-12-01 10:00:00","c-1003"), 
("i-104","85126A","VWA",102,5,"2021-12-01 10:31:00","c-1004"), 
("i-105","85127A","AAS",100,7,"2021-12-01 10:45:00","c-1005"), 
("i-106","85128A","FAS",130,3,"2021-12-01 11:06:00","c-1006"), 
("i-107","85129A","AFA",175,6,"2021-12-01 11:15:00","c-1007"), 
("i-108","85130A","GAG",150,8,"2021-12-01 11:46:00","c-1008"), 
("i-109","85131A","AGG",180,8,"2021-12-01 12:56:00","c-1009"), 
("i-110","85132A","KKK",200,1,"2021-12-01 14:36:00","c-1010")) 
columns= ["invoice_no","product_code","descr","unit_price","quantity","invoice_date","customer_id"] 
df = spark.createDataFrame(data = simpleData, schema = columns) 

# COMMAND ----------

df.show()

# COMMAND ----------

#Purchase by customers per hour/Total purchase by hour. 
#df1=df.groupBy(hour("invoice_date")).agg(sum_of_col(col("quantity")*col("unit_price")).alias("Total_purchase"))
#df1.show()
df1=df.groupBy(hour("invoice_date"))
#df1.show()

# COMMAND ----------

#Top 3 customer purchase 
df.select("customer_id").orderBy(desc(col("quantity")*col("unit_price"))).show(3)

# COMMAND ----------

#Total sales by year 
df.groupBy(year("invoice_date")).agg(sum_of_col(col("quantity")*col("unit_price"))).alias("Total_Sales").show()

# COMMAND ----------

#Total sales by month
df.groupBy(month("invoice_date")).agg(sum_of_col(col("quantity")*col("unit_price"))).alias("Total_Sales").show()


# COMMAND ----------

#Total sales by quarter
#df4 = df.groupBy(quarter("invoice_date").alias("quarter")).agg(sum_of_col(col("quantity")*col("unit_price"))).alias("Total_Sales")
df.sql("select 'Spark' as hello").show()

# COMMAND ----------

#Sort based on sales 
df2=df.withColumn("sales",col("quantity")*col("unit_price"))
df2.sort(col("sales").desc()).show()

# COMMAND ----------

import datetime
from datetime import datetime

for i in df2.collect():
    datetime_obj = datetime.strptime(i["invoice_date"], 
                                 "%Y-%m-%d %H:%M:%S")
    curr_hour = datetime_obj.time().hour
    if curr_hour <= 23:
        print(curr_hour)
    else:
        print(0)

# COMMAND ----------

#created dataframe using current date
df2=spark.createDataFrame([["2022-04-08"]],["current_date"])

# COMMAND ----------

df2.show()

# COMMAND ----------

from datetime import date
from pyspark.sql.functions import date_add,date_sub,trunc

# COMMAND ----------

print(date.today())

# COMMAND ----------

df2.withColumn("addDate",date_add(df2.current_date,5)).show()

# COMMAND ----------

#sub 5 days from current date
df2.withColumn("subDate",date_sub(df2.current_date,5)).show()

# COMMAND ----------

#first day of this month
df2.withColumn("startMonthDate",trunc(df2.current_date,"month")).show()

# COMMAND ----------

df.select(concat_ws("-",year(df.invoice_date),quarter(df.invoice_date))).show()

# COMMAND ----------


