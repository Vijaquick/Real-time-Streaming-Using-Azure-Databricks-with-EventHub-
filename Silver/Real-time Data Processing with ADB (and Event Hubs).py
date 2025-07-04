# Databricks notebook source
# MAGIC %md
# MAGIC #### Silver Layer
# MAGIC
# MAGIC -Reading, transforming and writing the stream from the bronze to the silver layer.

# COMMAND ----------

# DBTITLE 1,Transformation and flattening Json

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType,DataType
from pyspark.sql.functions import *
df=spark.readStream.format('delta').table("streaming.bronze.orders")
convert_string=df.withColumn("body", col("body").cast("string")) # convert as a string
#define schema
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("shipping_address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("pincode", StringType(), True)
    ]), True),
    StructField("items", ArrayType(StructType([
        StructField("item_id", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price_per_unit", DoubleType(), True)
    ])), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("delivery_status", StringType(), True)
])

#apply schema to the json data

df2=convert_string.withColumn("body",from_json(col("body"), order_schema))
select_columns=df2.select("body.*")  


customers=select_columns.select("customer_id","customer_name")


shipping_add=select_columns.withColumn("street",col("shipping_address.street"))\
.withColumn("city",col("shipping_address.city"))\
.withColumn("state",col("shipping_address.state"))\
.withColumn("pincode",col("shipping_address.pincode"))\
.withColumn("customer_id", col("customer_id"))\
.withColumn("customer_name", col("customer_name"))\
.withColumn("order_id", col("order_id"))
shipping_address=shipping_add.select("street","city","state","pincode","customer_id","customer_name","order_id")

items=select_columns.withColumn("item_id",explode(col("items.item_id")))\
.withColumn("item_name",explode(col("items.item_name")))\
.withColumn("category",explode(col("items.category")))\
.withColumn("quantity",explode(col("items.quantity")))\
.withColumn("price_per_unit",explode(col("items.price_per_unit")))\
.withColumn("order_id", col("order_id"))\
.withColumn("customer_id", col("customer_id"))\
.withColumn("customer_name", col("customer_name"))\
.withColumn("order_date", col("order_date").cast("date"))

order_items=items.select("item_id","item_name","category","quantity","price_per_unit","order_id","customer_id","customer_name","order_date")

payment1=select_columns.withColumn("payment_method", col("payment_method"))\
.withColumn("payment_status", col("payment_status"))\
.withColumn("delivery_status", col("delivery_status"))\
.withColumn("order_id", col("order_id"))\
.withColumn("customer_id", col("customer_id"))\
.withColumn("customer_name", col("customer_name"))\
.withColumn("total_amount", col("total_amount"))\
    .withColumn("payment_method", col("payment_method"))\
        .withColumn("payment_status", col("payment_status"))\
            .withColumn("delivery_status", col("delivery_status"))\
         .withColumn("order_date", col("order_date").cast("date"))

payment=payment1.select("payment_method","payment_status","delivery_status","order_id","customer_id","customer_name","total_amount","order_date").dropDuplicates(['order_id'])

# Writing stream: Save the transformed data to the 'streaming.silver.weather' Delta table in 'append' mode with checkpointing for data reliability
customers_query = customers.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/streaming/silver/customers") \
    .toTable("streaming.silver.customers")

order_items_query = order_items.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/streaming/silver/items") \
    .toTable("streaming.silver.items")

shipping_address_query = shipping_address.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/streaming/silver/shipping_address") \
    .toTable("streaming.silver.shipping_address")

payment_query = payment.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/streaming/silver/payment") \
    .toTable("streaming.silver.payment")

payment_query.awaitTermination()
