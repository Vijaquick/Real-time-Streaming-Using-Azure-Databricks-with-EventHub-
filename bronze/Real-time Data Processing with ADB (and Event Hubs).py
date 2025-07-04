# Databricks notebook source
# MAGIC %md
# MAGIC # Real-time Data Processing with Azure Databricks (and Event Hubs)
# MAGIC
# MAGIC - Data Sources: Streaming data from IoT devices or social media feeds. (Simulated in Event Hubs)
# MAGIC - Ingestion: Azure Event Hubs for capturing real-time data.
# MAGIC - Processing: Azure Databricks for stream processing using Structured Streaming.
# MAGIC - Storage: Processed data stored Azure Data Lake (Delta Format).
# MAGIC - Visualisation: Data visualized using Power BI.
# MAGIC
# MAGIC | Developed By            | LastUpdate  |               Team        |
# MAGIC |-----------------|-------------|---------------------------|
# MAGIC | Vignesan        | 04-07-2025  | Data Team                 |
# MAGIC
# MAGIC ### Azure Databricks Configuration Required
# MAGIC - Single Node Compute Cluster: `12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)`
# MAGIC - Maven Library installed on Compute Cluster: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create Schema
try:
    spark.sql("create catalog streaming;")
except:
    print('check if catalog already exists')

try:
    spark.sql("create schema streaming.bronze;")
except:
    print('check if bronze schema already exists')

try:
    spark.sql("create schema streaming.silver")
except:
    print('check if silver schema already exists')

try:
    spark.sql("create schema streaming.gold;")
except:
    print('check if gold schema already exists')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC Set up Azure Event hubs connection string.

# COMMAND ----------

# DBTITLE 1,EventHub Config
# Config
# Replace with your Event Hub namespace, name, and key
connectionString = "sharedkey"
eventHubName = "streaming"

ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
  'eventhubs.eventHubName': eventHubName
}

# COMMAND ----------

# MAGIC %md
# MAGIC Reading and writing the stream to the bronze layer.

# COMMAND ----------

# DBTITLE 1,Read Streaming Data
# Reading stream: Load data from Azure Event Hub into DataFrame 'df' using the previously configured settings
df = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load() \

# Displaying stream: Show the incoming streaming data for visualization and debugging purposes
df.display()



# COMMAND ----------

# DBTITLE 1,Write Streaming Data
# Writing stream: Persist the streaming data to a Delta table 'streaming.bronze.weather' in 'append' mode with checkpointing
adf=df.writeStream\
    .option("checkpointLocation", "/mnt/streaming/bronze/orders")\
    .outputMode("append")\
    .format("delta")\
    .toTable("streaming.bronze.orders")
adf.awaitTermination()
