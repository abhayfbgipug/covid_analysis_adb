# Databricks notebook source
import requests
response_API = requests.get('https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv')
print(response_API.status_code)
data = response_API.text
type(data)
print(data)
df=spark.read.csv(data)
display(df)


# COMMAND ----------

import requests
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

# Make the HTTP request
response_API = requests.get('https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv')

# Check the status code
print(response_API.status_code)

# Read the CSV data
data = response_API.text

# Initialize a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Define the schema based on your data
schema = StructType([
    StructField("country", StringType(), True),
    StructField("indicator", StringType(), True),
    StructField("date", DateType(), True),
    StructField("year_week", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("source", StringType(), True),
    StructField("url", StringType(), True),
])

# Read the CSV data into a Spark DataFrame
df = spark.read.csv(StringIO(data), header=True, schema=schema)

# Display the DataFrame
df.show()


# COMMAND ----------


