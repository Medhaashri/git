# Databricks notebook source


# COMMAND ----------

df=spark.read.option("multiline","True").json("/FileStore/tables/tripdetail_json-1.json")
df

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.select(df.tpep_dropoff_datetime,df.tpep_pickup_datetime).show()


# COMMAND ----------

#Convert the tpep_pickup_datetime, tpep_dropoff_datetime both columns into IST from PST and add as a seperate column with _IST suffix.
from pyspark.sql.functions import *

# COMMAND ----------

df=df.withColumn("tpep_dropoff_datetime_UTC",to_utc_timestamp(df.tpep_dropoff_datetime,"PST")).withColumn("tpep_pickup_datetime_UTC",to_utc_timestamp(df.tpep_pickup_datetime,"PST"))

# COMMAND ----------

df=df.withColumn("tpep_dropoff_datetime_IST",from_utc_timestamp(df.tpep_dropoff_datetime_UTC,"IST")).withColumn("tpep_pickup_datetime_IST",to_utc_timestamp(df.tpep_pickup_datetime_UTC,"IST"))

# COMMAND ----------

display(df)

# COMMAND ----------

df.select(df.tpep_dropoff_datetime_IST,df.tpep_pickup_datetime_IST).show()

# COMMAND ----------

df=df.drop("tpep_dropoff_datetime_UTC","tpep_pickup_datetime_UTC")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumn("tpep_dropoff_datetime",df.tpep_dropoff_datetime.cast("timestamp"))

# COMMAND ----------

df=df.withColumn("tpep_pickup_datetime",df.tpep_pickup_datetime.cast("timestamp"))

# COMMAND ----------

df.printSchema

# COMMAND ----------

df=df.withColumn("TravelTime",df.tpep_dropoff_datetime.cast('long')-df.tpep_pickup_datetime.cast('long'))

# COMMAND ----------

df.select("TravelTime").show()

# COMMAND ----------

#5. Build a temp view on top of the data frame

#6. Write spark sql query to aggregate fare_amount based on Vendor Id.
df.createOrReplaceTempView("temptable")
df2=spark.sql("select VendorID,round(sum(fare_amount),2) as sum_fare_amount from temptable group by 1")
df2.show()

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.coalesce(1).write.mode("overwrite").option("header","true").csv("/FileStore/tables/temp.csv")
display(df2)

# COMMAND ----------

# partition by 3
df=df.withColumn("day",to_date("tpep_dropoff_datetime"))
display(df)

# COMMAND ----------

df.write.option("header",True) \
        .option("maxRecordasPerFile",171) \
.partitionBy("day") \
.mode("overwrite") \
.csv("/Filestore/tables/tripdetail_csv.csv")


# COMMAND ----------


