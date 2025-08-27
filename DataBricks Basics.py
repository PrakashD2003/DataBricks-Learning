# Databricks notebook source
# MAGIC %md
# MAGIC # Starting With DataBricks

# COMMAND ----------

# Creating dataframe in Databricks
data = [(1,"prakash",99),(2,"suresh",98),(3,"ramesh",97)]
my_schema = ["id","name","marks"]
df = spark.createDataFrame(data=data,schema=my_schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Data From DataLake
# MAGIC - App ID = ""
# MAGIC - Tenant Id = ""
# MAGIC - secret = ""

# COMMAND ----------

# Reading data from Azure Data Lake(Authentication using Service Principal)
spark.conf.set("fs.azure.account.auth.type.datalakelearningpd.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalakelearningpd.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalakelearningpd.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.secret.datalakelearningpd.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakelearningpd.dfs.core.windows.net", "https://login.microsoftonline.com//oauth2/token")

# Example path to read data
df_datalake = spark.read.format("csv").option("header", "true").load("abfss://source@datalakelearningpd.dfs.core.windows.net/Sales.csv")
display(df_datalake)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databrick Utils(dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dbutils.fs → browse raw data in DBFS or mounted cloud buckets.

# COMMAND ----------

dbutils.fs.ls("abfss://source@datalakelearningpd.dfs.core.windows.net/") # List all files in the directory

# COMMAND ----------

# MAGIC %md 
# MAGIC ### dbutils.widgets → make notebooks reusable for different parameters.

# COMMAND ----------

dbutils.widgets.text("ID", "1") # Create a widget to enter the ID value as input to the notebook and use it in the notebook as a variable
# "ID" is the name of the widget and "1" is the default value
var = dbutils.widgets.get("ID") # Get the value of the widget and use it in the notebook as a variable
print(var)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dbutils.secrets → securely use keys for APIs/databases.

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="Databricks-Learning-Scope")

# COMMAND ----------

dbutils.secrets.get(scope = "Databricks-Learning-Scope", key = "DataBricks-App-Secret")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("abfss://source@datalakelearningpd.dfs.core.windows.net/Sales.csv") 
    # Read data from Azure Data Lake Storage. 
    #"format()" is used to specify the format of the data. 
    #"option()" is used to specify the options for the format. 
    # option("header","true") is used to specify that the data has a header row.
    # option("inferSchema","true") is used to specify that the data schema should be inferred from the data. Spark will automatically infer the data types of the columns based on the data in the columns. 
    #"load()" is used to load the data from the specified path.
# spark.read.csv("abfss://source@datalakelearningpd.dfs.core.windows.net/Sales.csv") is also valid

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Transformations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_new = df.withColumn( "Item_type",split(col("Item_Type"), " "))
display(df_new)# Split the Item_Type column values such that sentences are separated by a space so each word becomes an item in a list


# COMMAND ----------

df_new = df_new.withColumn("ID",lit(var))# Add a new column with the value you specify in this case we have used the value of the widget
display(df_new)

# COMMAND ----------

from pyspark.sql.types import IntegerType
df_new = df_new.withColumn("Item_Weight",col("Item_Weight").cast(IntegerType()))
# Change the data type of the Item_Weight column to integer

display(df_new)

# COMMAND ----------


