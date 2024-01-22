# Databricks notebook source
ADLS_SP_NAME=dbutils.secrets.get(scope = "0ff9d649-bf63-4fc0-a4a9-2784e5a869ea" , key = "data-client-id")
ADLS_SP_KEY=dbutils.secrets.get(scope = "vpV8Q~7LFX9dv2w~PNDo59yPqkhvPjrIRpopgaSv" , key = "data-client-secret")
REFRESH_URL=dbutils.secrets.get(scope = "https://login.microsoftonline.com/05d75c05-fa1a-42e7-9cf1-eb416c396f2d/oauth2/token" , key = "data-client-refresh-url")
ETP_LOB_URL=dbutils.secrets.get(scope = "etp_secrets" , key = "lob-mount-url")
ETP_PUB_URL=dbutils.secrets.get(scope = "etp_secrets" , key = "pub-mount-url")
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id":ADLS_SP_NAME,
           "fs.azure.account.oauth2.client.secret":ADLS_SP_KEY,
           "fs.azure.account.oauth2.client.endpoint": REFRESH_URL }


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "0ff9d649-bf63-4fc0-a4a9-2784e5a869ea",
          "fs.azure.account.oauth2.client.secret": "vpV8Q~7LFX9dv2w~PNDo59yPqkhvPjrIRpopgaSv",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7159a550-6858-4294-beb6-3d94c2a1346a/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(source = "abfss://raw@covidrepodl1.dfs.core.windows.net/",mount_point = "/mnt/rawtest/",extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/rawtest/

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.covidrepodl1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.covidrepodl1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.covidrepodl1.dfs.core.windows.net", "sp=r&st=2024-01-17T12:42:14Z&se=2024-01-17T20:42:14Z&spr=https&sv=2022-11-02&sr=c&sig=Rpib0vgeILNd1Purc4abQZNYlw3oglamJr0dEzHUZyw%3D")

# COMMAND ----------

spark.read.csv("abfss://raw@covidrepodl1.dfs.core.windows.net/population/data.csv")
#dbutils.fs.ls("abfss://raw@covidrepodl1.dfs.core.windows.net/")


# COMMAND ----------


mountPoint_pub = "/mnt/datalake_etp_pub/"
pub_source =ETP_PUB_URL
dbutils.fs.mount(source = pub_source,mount_point = mountPoint_pub,extra_configs = configs)

# COMMAND ----------

mountPoint = "/mnt/prod_cleansed/sfdc/"
source ="abfss://sfdc@dlsazewpdatalakecleansed.dfs.core.windows.net"
dbutils.fs.mount(source = source,mount_point = mountPoint,extra_configs = configs)

# COMMAND ----------

mountPoint = "/mnt/adls-prod-pub-oceanpnl-gen2/"
source ="abfss://oceanpnl@dlsazewpdatalakepublish.dfs.core.windows.net"
dbutils.fs.mount(source = source,mount_point = mountPoint,extra_configs = configs)

# COMMAND ----------

mountPoint = "/mnt/prod_costmanagement/"
source ="abfss://costmanagement@dlsazewpdatalakepublish.dfs.core.windows.net"
dbutils.fs.mount(source = source,mount_point = mountPoint,extra_configs = configs)

# COMMAND ----------

mountPoint = "/mnt/prod_lob_shippingcycle/"
source ="abfss://shippingcycle@dlsazewpdatalakelob.dfs.core.windows.net"
dbutils.fs.mount(source = source,mount_point = mountPoint,extra_configs = configs)

# COMMAND ----------

mountPoint = "/mnt/datalake_prod_lob_synapsetoadls_gen2/"
source ="abfss://synapsetoadls@dlsazewpdatalakelob.dfs.core.windows.net"
dbutils.fs.mount(source = source,mount_point = mountPoint,extra_configs = configs)

# COMMAND ----------

mountPoint = "/mnt/prod_ontology_customer/"
source ="abfss://customer@dlsazewpdatalakecurated.dfs.core.windows.net"
dbutils.fs.mount(source = source,mount_point = mountPoint,extra_configs = configs)

# COMMAND ----------

mountPoint = "/mnt/prod_finontology/"
source ="abfss://finontology@dlsazewpdatalakelob.dfs.core.windows.net"
dbutils.fs.mount(source = source,mount_point = mountPoint,extra_configs = configs)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table lobetp.ets as (select * from delta.`/mnt/prod_finontology/refined/sslmdm/tf_EquipTimeSlice_a/` where eventDate >='2021-10-01' and eventDate <='2023-12-31');

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/datalake_etp_lob/ecodelivery/tradefactor")

# COMMAND ----------


mountPoint_lob = "/mnt/datalake_etp_dev_lob/"
lob_source ="abfss://etp@dlsazewddatalakelob.dfs.core.windows.net"
dbutils.fs.mount(source = lob_source,mount_point = mountPoint_lob,extra_configs = configs)
