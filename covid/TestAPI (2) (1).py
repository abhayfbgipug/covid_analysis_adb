# Databricks notebook source
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists sales;

# COMMAND ----------

salesdf=spark.read.format("csv").option("header",True).load("/FileStore/sales_data.csv")\
                  .withColumn("price",col("price").cast("decimal(13,4)"))\
                  .withColumn("product_id",trim(col("product_id")).cast("int"))\
                  .withColumn("customer_id",trim(col("customer_id")).cast("int"))
display(salesdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE sales.salesData;
# MAGIC create table sales.salesData(
# MAGIC order_id string,
# MAGIC customer_id int,
# MAGIC product_id int,
# MAGIC quantity string,
# MAGIC price decimal(13,4),
# MAGIC order_date string)
# MAGIC using parquet
# MAGIC Location '/user/hive/warehouse/sales.db/salesdata'
# MAGIC

# COMMAND ----------

salesdf.write.mode("overwrite").parquet("/user/hive/warehouse/sales.db/salesdata")

# COMMAND ----------

https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv

# COMMAND ----------

response_API = requests.get('https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv')
print(response_API.status_code)
data = response_API.text
type(data)
#print(data)
#df=spark.read.csv(data)
#display(df)

# COMMAND ----------

import string
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark import SparkFiles

# COMMAND ----------

data_file_https_url = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"
sc.addFile(data_file_https_url)
filePath  = 'file://' +SparkFiles.get('data.csv')
citiesDf = spark.read.csv(filePath, header=True, inferSchema= True)
display(citiesDf)

# COMMAND ----------


response_API = requests.get('https://jsonplaceholder.typicode.com/users')
print(response_API.status_code)
data = response_API.text
json_data=json.loads(data)
json_rdd = sc.parallelize([data])
df=spark.read.json(json_rdd)
customerdf=df.select("id","name","username","email","address.city","address.geo.lat","address.geo.lng")
display(customerdf)


# COMMAND ----------

customerdf.write.mode("overwrite").parquet("/user/hive/warehouse/sales.db/customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE sales.customer;
# MAGIC create table sales.customer(
# MAGIC id long,
# MAGIC name string,
# MAGIC username string,
# MAGIC email string,
# MAGIC city string,
# MAGIC lat string,
# MAGIC lng string)
# MAGIC using parquet
# MAGIC Location '/user/hive/warehouse/sales.db/customer'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.salesdata 

# COMMAND ----------

# DBTITLE 1,Function to pull data from api based on lat,long
def weather(lat,lon):
  api_key="02aec3d8e4f0de862fd7720c5f2264f0"
  query_params = {"api_key": api_key}
  url="https://api.openweathermap.org/data/2.5/weather?lat="+lat+"&lon="+lon+"&APPID="+api_key
  response_API = requests.get(url)
  print(response_API.status_code)
  data = response_API.text
  json_data=json.loads(data)
  lon=json_data["coord"]["lon"]
  pressure=json_data["main"]["pressure"]
  temperature=json_data["main"]["temp"]
  humidity=json_data["main"]["humidity"]
  wind_speed=json_data["wind"]["speed"]
  return[pressure,temperature,humidity,wind_speed]
convertUDF = udf(weather,ArrayType(StringType()))

# COMMAND ----------

customer_weatherDf=customerdf.withColumn("humidity",convertUDF(col("lat"),col("lng"))[2])\
             .withColumn("Temperature",convertUDF(col("lat"),col("lng"))[1])\
             .withColumn("Pressure",convertUDF(col("lat"),col("lng"))[0])\
             .withColumn("Wind_speed",convertUDF(col("lat"),col("lng"))[3])\
             .withColumn("Temperature",trim(col("Temperature")).cast("decimal(10,4)"))

# COMMAND ----------

customer_weatherDf.write.mode("overwrite").parquet("/user/hive/warehouse/sales.db/weather")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE sales.customer;
# MAGIC create table sales.customer_weather(
# MAGIC id long,
# MAGIC name string,
# MAGIC username string,
# MAGIC email string,
# MAGIC city string,
# MAGIC lat string,
# MAGIC lng string,
# MAGIC humidity string,
# MAGIC Temperature decimal(10,4),
# MAGIC Pressure string,
# MAGIC Wind_speed string)
# MAGIC using parquet
# MAGIC Location '/user/hive/warehouse/sales.db/weather'

# COMMAND ----------

cust_sales=salesdf.join(customer_weatherDf,salesdf.customer_id==customerdf.id,"inner").drop(customerdf.id)\
                  .withColumn("sale_amount",col("quantity")*col("price"))
cust_sales.count()

# COMMAND ----------

cust_sales.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales.dataset

# COMMAND ----------

cust_sales.write.mode("overwrite").saveAsTable("sales.dataset")

# COMMAND ----------

# DBTITLE 1,Final Dataset for analysis
# MAGIC %sql
# MAGIC select * from sales.dataset

# COMMAND ----------

# DBTITLE 1,Total sales per customer
sales_per_customer=cust_sales.groupBy("customer_id","name").agg(sum(col("sale_amount")).alias("Total_sale"))

# COMMAND ----------

sales_per_customer.write.mode("overwrite").saveAsTable("sales.sales_per_customer")

# COMMAND ----------

sales_per_customer.display()

# COMMAND ----------

# DBTITLE 1,Average order quantity per product
Average_quantity_per_product=cust_sales.groupBy("product_id").agg(avg(col("quantity")).alias("Average_quantity_per_product")).orderBy("product_id")

# COMMAND ----------

Average_quantity_per_product.write.mode("overwrite").saveAsTable("sales.Average_quantity_per_product")

# COMMAND ----------

display(Average_quantity_per_product)

# COMMAND ----------

# DBTITLE 1,Top-selling products per customers
quantity_perproduct=cust_sales.groupBy("customer_id","product_id").agg(sum("quantity").alias("Total_quantity")).orderBy("customer_id")
#display(quantity_perproduct)
WindowSpec=Window.partitionBy("customer_id")
top_selling_df=quantity_perproduct.withColumn("rank_product",dense_rank().over(WindowSpec.orderBy(desc("Total_quantity")))).filter(col("rank_product")==1).withColumn("Top_selling_product_id",col("product_id")).select("customer_id","Top_selling_product_id","Total_quantity")
display(top_selling_df)

# COMMAND ----------

top_selling_df.write.mode("overwrite").saveAsTable("sales.Top_selling_pro_percustomer")

# COMMAND ----------

  lat=str(-43.9509)
  lon=str(34.4618)
  api_key="02aec3d8e4f0de862fd7720c5f2264f0"
  query_params = {"api_key": api_key}
  url="https://api.openweathermap.org/data/2.5/weather?lat="+lat+"&lon="+lon+"&APPID="+api_key
  response_API = requests.get(url)
  print(response_API.status_code)
  data = response_API.text
  json_data=json.loads(data)
  print(json_data)
  #lon=json_data["coord"]["lon"]
  pressure=json_data["main"]["pressure"]
  temperature=json_data["main"]["temp"]
  humidity=json_data["main"]["humidity"]
  wind_speed=json_data["wind"]["speed"]

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Analyze Sales Dataset for Monthly or Quarterly basis
df=spark.sql("select * from sales.dataset")

# COMMAND ----------

sales_trend=df.withColumn("order_date",to_date("order_date")).withColumn("Month",month(col("order_date"))).withColumn("year",year(col("order_date"))).withColumn("quarter",quarter(col("order_date")))
display(sales_trend)
sales_trend.createOrReplaceTempView("sales_tab")

# COMMAND ----------

# DBTITLE 1,Sales_per_month_of_every_product of each year
# MAGIC %sql
# MAGIC select product_id,Month,Year,sum(sale_amount) as Sale_per_month from sales_tab group by product_id,Month,Year

# COMMAND ----------

# DBTITLE 1,Sales_per_quarter_of_every_product of each year
# MAGIC %sql
# MAGIC select product_id,Quarter,Year,sum(sale_amount) as Sale_per_quarter from sales_tab group by product_id,Quarter,Year

# COMMAND ----------

# DBTITLE 1,Total sales per month
# MAGIC %sql
# MAGIC select Month,Year,sum(sale_amount) as Total_Sale_per_month from sales_tab group by Month,Year

# COMMAND ----------

# DBTITLE 1,Total sales per quarter where temperature is greater than 265
# MAGIC %sql
# MAGIC select Quarter,Year,sum(sale_amount) as Total_Sale_per_quarter from sales_tab where temperature>265 group by Quarter,Year 
