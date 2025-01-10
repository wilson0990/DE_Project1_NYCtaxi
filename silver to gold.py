# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect Databricks to adls

# COMMAND ----------

# Define values
storage_account = "adlsfornyctaxi"
storage_container = "silver"
application_id = 'paste your application id here'
secret = 'paste your secret here'
tenant_id = 'paste your tenant id here'


# Configure Spark
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token")

# Attempt to list directory contents
path = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/2023"
dbutils.fs.ls(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the Parquet files recusively
# MAGIC > Here we are using the recusiveFileLookup, because in the silver container we processed data to be datatypes are in the required formate, here it won't create a error.

# COMMAND ----------

df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").option("recursiveFileLookup", "true").load(path)

df.display()

# checking whether we got all the expected rows count
df.select("*").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing the Data into the Gold Layer
# MAGIC > Once the data is pushed to Gold we create a delta tables on top it.
# MAGIC > To store the table we need the database as well, we will create in the next step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating database name as gold to save the delta tables in it.
# MAGIC create database gold

# COMMAND ----------

path = f"abfss://gold@{storage_account}.dfs.core.windows.net/2023"
df.write.format("delta").mode("append").option("path", path).saveAsTable("gold.nyctaxidata")

# COMMAND ----------

# MAGIC %md
# MAGIC > Since we have created the table, now we can also query that table using sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.nyctaxidata

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Delta Lake
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- versioning
# MAGIC -- Let me update the row or column or table, it will store as versions and later we can rollback to previous version.
# MAGIC
# MAGIC update gold.nyctaxidata set passenger_count = 1 where passenger_count = 0
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the `Versions`

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.nyctaxidata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Go back to the Previous Version

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE gold.nyctaxidata TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check we have the passenger count where it is 0
# MAGIC select * from gold.nyctaxidata where passenger_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Table 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold.nyctaxidata

# COMMAND ----------

# MAGIC %md
# MAGIC > The next task is connect the databricks with the Powerbi to create the dashboard.
