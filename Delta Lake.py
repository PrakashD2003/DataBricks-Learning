# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### %run Magic Command
# MAGIC - The %run command is used to execute another notebook within the current notebook.
# MAGIC - It allows you to modularize your code by separating it into different notebooks.
# MAGIC - Here, it runs the notebook located at "/Workspace/DataBricks Learning/DataBricks Basics".
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/DataBricks Learning/DataBricks Basics"
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Tables

# COMMAND ----------

# Writing Data to Delta Lake
# Using 'delta' format to ensure ACID transactions and scalable metadata handling
# 'append' mode is used to add new data to the existing Delta table
# Specifying the path to the Delta Lake location in Azure Data Lake Storage
# Note: The Delta table is created in Delta format, which supports ACID transactions, scalable metadata handling, and efficient data processing.
# A Delta table is a table stored in the Delta Lake format, which provides features like ACID transactions, schema enforcement, and time travel.
df_new.write.format("delta").mode("append").option("path", "abfss://destination@datalakelearningpd.dfs.core.windows.net/sales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Managed Vs. External Delta Table**
# MAGIC
# MAGIC - **Managed Table**: 
# MAGIC   - The data and metadata are managed by Delta Lake.
# MAGIC   - When you drop a managed table, both the table metadata and the data are deleted.
# MAGIC   - Typically stored in the default location of the database.
# MAGIC
# MAGIC - **External Table**: 
# MAGIC   - Only the metadata is managed by Delta Lake, while the data is stored externally.
# MAGIC   - Dropping an external table only removes the metadata, not the data itself.
# MAGIC   - Useful for managing data stored in external systems or specific locations.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating Database 
# MAGIC Create Database if not exists Student;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating Managed Table inside Database
# MAGIC Create Table if not exists Student.Managed_Student_Info_Table
# MAGIC (
# MAGIC   Student_ID int,
# MAGIC   Student_Name string,
# MAGIC   Student_Age int
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Inserting Data into Managed Table
# MAGIC INSERT INTO Student.Managed_Student_Info_Table
# MAGIC VALUES 
# MAGIC (1,'John',25),
# MAGIC (2,'Jane',23),
# MAGIC (3,'Mary',27),
# MAGIC (4,'Mike',22),
# MAGIC (5,'Sara',26)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Querying Our DATABASE
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table

# COMMAND ----------

# MAGIC %sql
# MAGIC --Droping Our Managed Table
# MAGIC --DROP TABLE Student.Managed_Student_Info_Table
# MAGIC --This will delete the metadata and the data both from our metastore and the storage layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### External Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a storage credential using Azure Service Principal for secure access
# MAGIC CREATE STORAGE CREDENTIAL my_adls_credential
# MAGIC WITH AZURE_SERVICE_PRINCIPAL (
# MAGIC   client_id = '',  -- Replace with your Azure application client ID
# MAGIC   tenant_id = '',    -- Replace with your Azure directory tenant ID
# MAGIC   client_secret = ''       -- Replace with your Azure client secret
# MAGIC );
# MAGIC
# MAGIC -- Register an external location in Databricks to access data in Azure Data Lake Storage
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS StudentExternalLocation
# MAGIC URL 'abfss://destination@datalakelearningpd.dfs.core.windows.net/Student_Database'
# MAGIC WITH (
# MAGIC   STORAGE CREDENTIAL my_adls_credential  -- Use the created storage credential for authentication
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create the external table
# MAGIC CREATE TABLE IF NOT EXISTS Student.External_Student_Info_Table (
# MAGIC   Student_ID INT,
# MAGIC   Student_Name STRING,
# MAGIC   Student_Age INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://destination@datalakelearningpd.dfs.core.windows.net/Student_Database';

# COMMAND ----------

# MAGIC %sql
# MAGIC --Inserting Data into External Table
# MAGIC INSERT INTO Student.External_Student_Info_Table
# MAGIC VALUES 
# MAGIC (1,'John',25),
# MAGIC (2,'Jane',23),
# MAGIC (3,'Mary',27),
# MAGIC (4,'Mike',22),
# MAGIC (5,'Sara',26)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Querying Our DATABASE
# MAGIC SELECT * FROM Student.External_Student_Info_Table

# COMMAND ----------

# MAGIC %md
# MAGIC #  Delta Table Funtionalities

# COMMAND ----------

# MAGIC %md
# MAGIC ### INSERT 
# MAGIC
# MAGIC - The INSERT statement is used to add new rows to a table.
# MAGIC - You can insert data into a Delta table using SQL or DataFrame API.
# MAGIC - Supports inserting data from another table or a query.
# MAGIC - Ensures ACID transactions, so inserts are atomic and consistent.
# MAGIC - Can be used with the `INSERT INTO` or `INSERT OVERWRITE` syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Inserting Data into Managed Table
# MAGIC INSERT INTO Student.Managed_Student_Info_Table
# MAGIC VALUES 
# MAGIC (6,'Alex',25),
# MAGIC (7,'Emma',23),
# MAGIC (8,'Liam',27),
# MAGIC (9,'Olivia',22),
# MAGIC (10,'Noah',26)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Querying Our DATABASE
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELETE
# MAGIC
# MAGIC - The DELETE statement is used to remove rows from a table.
# MAGIC - You can delete data from a Delta table using SQL or DataFrame API.
# MAGIC - Supports deleting data based on a condition.
# MAGIC - Ensures ACID transactions, so deletes are atomic and consistent.
# MAGIC - Can be used with the `DELETE FROM` syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM Student.Managed_Student_Info_Table WHERE Student_ID = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tombstoning
# MAGIC
# MAGIC - Tombstoning is the process of marking data files for deletion.
# MAGIC - Delta Lake uses tombstones to manage data deletion and retention.
# MAGIC - Tombstoned files are not immediately removed but are instead marked for deletion.
# MAGIC - These files are eventually removed during the vacuum operation.
# MAGIC - Helps in maintaining the integrity of the transaction log and data consistency.
# MAGIC - Useful for compliance with data retention policies.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Versioning
# MAGIC
# MAGIC - Delta Lake maintains a transaction log that records all changes to a table.
# MAGIC - Each write operation creates a new version of the table.
# MAGIC - You can query previous versions of a table using the version number or timestamp.
# MAGIC - Supports time travel to access historical data.
# MAGIC - Useful for auditing, debugging, and reproducing experiments.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Student.Managed_Student_Info_Table --Used to see the history of the table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel
# MAGIC
# MAGIC - Delta Lake allows you to access previous versions of a table.
# MAGIC - You can query historical data using a version number or a timestamp.
# MAGIC - Useful for auditing, debugging, and reproducing experiments.
# MAGIC - Supports `SELECT` queries with `VERSION AS OF` or `TIMESTAMP AS OF` clauses.
# MAGIC - Helps in recovering from accidental data changes or deletions.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Time travel to a specific version of the table
# MAGIC -- This command is used to Query the table to a specific version
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table VERSION AS OF 1 --When we added our first data from  ID 1 to 5

# COMMAND ----------

# MAGIC %sql
# MAGIC --This Command is used to restore the table to a specific version
# MAGIC RESTORE TABLE Student.Managed_Student_Info_Table TO VERSION AS OF 2 --When we Added our second data from ID 6 to 10 before Deleting ID 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table
# MAGIC --We can see that our table is restore the Version 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vacuum Command
# MAGIC
# MAGIC - The VACUUM command is used to remove old files from Delta Lake.
# MAGIC - It helps in reclaiming storage space by deleting files that are no longer needed.
# MAGIC - VACUUM can be configured with a retention period to protect against accidental data loss.
# MAGIC - The default retention period is 7 days to ensure data safety.
# MAGIC - It is important to ensure that no concurrent operations are accessing the data being vacuumed.
# MAGIC - Helps in maintaining the performance and efficiency of Delta Lake tables.
# MAGIC - **Impact on Time Travel and Data Versioning**:
# MAGIC   - VACUUM permanently deletes files older than the retention period, which can affect time travel.
# MAGIC   - Once files are vacuumed, you cannot query versions of the table older than the retention period.
# MAGIC   - Ensure that the retention period aligns with your data versioning and auditing requirements.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM Student.Managed_Student_Info_Table RETAIN 0 HOURS;
# MAGIC -- By default, VACUUM retains data for 7 days to ensure data recovery in case of accidental deletion.
# MAGIC -- RETAIN is used to specify the number of hours/days/weeks/months to retain the data
# MAGIC -- RETAIN 0 HOURS is used to delete all the data immediately from the table.
# MAGIC -- To change the default retention period, you can set the configuration 'spark.databricks.delta.retentionDurationCheck.enabled' to 'false'.
# MAGIC -- This allows you to specify a retention period less than the default 7 days.
# MAGIC
# MAGIC DESCRIBE HISTORY Student.Managed_Student_Info_Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Optimization
# MAGIC
# MAGIC - Delta Lake provides various optimization techniques to improve query performance.
# MAGIC - **Z-Ordering**: A technique to colocate related information in the same set of files. It helps in speeding up queries that involve filtering on specific columns.
# MAGIC - **Optimize Command**: Reorganizes the data in a Delta table to improve query performance. It compacts small files into larger ones, reducing the number of files to be read.
# MAGIC - **Data Skipping**: Automatically skips irrelevant data files based on the query predicates, reducing the amount of data scanned.
# MAGIC - **Caching**: Frequently accessed data can be cached in memory to speed up query execution.
# MAGIC - Regularly optimizing Delta tables can lead to significant performance improvements, especially for large datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Command
# MAGIC
# MAGIC - The OPTIMIZE command is used to reorganize the data in a Delta table.
# MAGIC - It compacts small files into larger ones, reducing the number of files to be read.
# MAGIC - Helps in improving query performance by reducing the overhead of opening many small files.
# MAGIC - Can be combined with Z-Ordering to colocate related information in the same set of files.
# MAGIC - Regularly running the OPTIMIZE command can lead to significant performance improvements, especially for large datasets.
# MAGIC - Syntax: `OPTIMIZE table_name [WHERE predicate] ZORDER BY (col1, col2, ...)`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table
# MAGIC --As we can see before optimizing the table query time is 2.61

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Student.Managed_Student_Info_Table 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table
# MAGIC --As we can see that bedore optimizing the table query time was 2.61 and after optimizing the table query time is reduced to 1.68

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-Ordering
# MAGIC
# MAGIC - Z-Ordering is a technique used in Delta Lake to colocate related information in the same set of files.
# MAGIC - It helps in speeding up queries that involve filtering on specific columns by reducing the amount of data scanned.
# MAGIC - Z-Ordering is particularly beneficial for large tables with frequent queries on specific columns.
# MAGIC - Can be used in conjunction with the OPTIMIZE command to improve query performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table
# MAGIC --As we can see before Z-Ordering the table query time is 1.80

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE Student.Managed_Student_Info_Table ZORDER BY Student_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Student.Managed_Student_Info_Table
# MAGIC --As we can see that bedore optimizing the table query time was 1.80 and after optimizing the table query time is reduced to 1.4(more usefull in large datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoLoader
# MAGIC
# MAGIC - AutoLoader is a feature in Databricks that incrementally and efficiently processes new data files as they arrive in cloud storage.
# MAGIC - It automatically detects new files and processes them without needing to manage file paths or schedules.
# MAGIC - Supports schema inference and evolution, allowing it to handle changes in data structure over time.
# MAGIC - Can be configured to use either a directory listing or a file notification service for detecting new files.
# MAGIC - Provides options for data deduplication and can be integrated with Delta Lake for ACID transactions.
# MAGIC - Useful for streaming data ingestion and ETL pipelines, ensuring data is always up-to-date.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Streaming Dataframe
# MAGIC
# MAGIC - A Streaming DataFrame is a continuous flow of data in Spark.
# MAGIC - It allows processing of real-time data streams using the same API as static DataFrames.
# MAGIC - Supports operations like filtering, aggregation, and joining on streaming data.
# MAGIC - Can be used with various data sources like Kafka, files, and sockets.
# MAGIC - Ensures exactly-once processing semantics for reliable data processing.
# MAGIC - Useful for building real-time analytics and monitoring applications.

# COMMAND ----------

# Reading streaming data using Auto Loader with cloudFiles source
# cloudFiles format is used by Auto Loader to efficiently process files arriving in cloud storage
df = (spark.readStream.format("cloudFiles")
    # Specify the format of the source files as Parquet
    .option("cloudFiles.format", "parquet")
    # Specify the location for storing the inferred schema
    .option("cloudFiles.schemaLocation", "abfss://auto-loader-destination@datalakelearningpd.dfs.core.windows.net/schema")
    # Load data from the specified Azure Data Lake Storage path
    .load("abfss://auto-loader-source@datalakelearningpd.dfs.core.windows.net"))

# COMMAND ----------

# Write the streaming DataFrame to a Delta table
(df.writeStream.format("delta")
# Specify the location for storing the checkpoint information
# Checkpointing is used to store the progress of the streaming query, allowing it to recover from failures
# It also keeps track of the data already loaded from the source, ensuring only new data is processed
.option("checkpointLocation", "abfss://auto-loader-destination@datalakelearningpd.dfs.core.windows.net/checkpoint")
# Set the trigger interval for processing the stream every 10 seconds
# Trigger defines the interval at which the streaming query processes new data
.trigger(processingTime="10 seconds")
# Start the streaming query and specify the output path in Azure Data Lake Storage
.start("abfss://auto-loader-destination@datalakelearningpd.dfs.core.windows.net/stream"))

# COMMAND ----------

