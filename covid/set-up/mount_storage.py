# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the following data lake storage gen2 containers
# MAGIC 1. raw
# MAGIC 2. processed
# MAGIC 3. lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set-up the configs
# MAGIC #### Please update the following 
# MAGIC - application-id
# MAGIC - service-credential
# MAGIC - directory-id

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "0c4a3560-bde3-46fd-b9b6-b6752f07d05b",
           "fs.azure.account.oauth2.client.secret": "5P58Q~DHCuS7gDs4PIDtiNzPiUnkGo1xzqs2CagZ",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7159a550-6858-4294-beb6-3d94c2a1346a/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the raw container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@covidrepodl1.dfs.core.windows.net/",
  mount_point = "/mnt/covidrepodl1/raw",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/covidrepodl1/raw/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the processed container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(source = "abfss://processed@covidrepodl1.dfs.core.windows.net/",mount_point = "/mnt/covidrepodl1/processed",extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the lookup container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://lookup@covidrepodl1.dfs.core.windows.net/",
  mount_point = "/mnt/covidrepodl1/lookup",
  extra_configs = configs)

# COMMAND ----------


