# Databricks notebook source
# MAGIC %md
# MAGIC #### Gold Layer
# MAGIC
# MAGIC  - Reading, aggregating and writing the stream from the silver to the gold layer

# COMMAND ----------

# DBTITLE 1,Aggreation Business Analytics
# Aggregating Stream: Read from 'streaming.silver.weather', apply watermarking and windowing, and calculate average weather metrics
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Aggregating Stream").getOrCreate()
payment = spark.readStream\
    .format("delta")\
    .table("streaming.silver.payment")
customers = spark.readStream\
    .format("delta")\
    .table("streaming.silver.customers")

items = spark.readStream\
    .format("delta")\
    .table("streaming.silver.items")

display(payment)

# COMMAND ----------

from pyspark.sql.functions import sum, expr,col

# Apply watermark on the base streaming DataFrames (before aggregation)
payment = payment.withColumn("order_date", col("order_date").cast("timestamp"))
items = items.withColumn("order_date", col("order_date").cast("timestamp"))

payment_with_watermark = payment.withWatermark("order_date", "1 day")
items_with_watermark = items.withWatermark("order_date", "1 day")

# Aggregation 1: Customer Spend
customer_spend = payment_with_watermark.groupBy("customer_id", "customer_name") \
    .agg(sum("total_amount").alias("total_spent"))

# Aggregation 2: Item Sales
item_sales = items_with_watermark.groupBy("item_id", "item_name", "category") \
    .agg(sum("quantity").alias("total_quantity_sold"))

# Aggregation 3: Item Revenue
item_revenue = items_with_watermark.withColumn("revenue", expr("quantity * price_per_unit")) \
    .groupBy("item_id", "item_name", "category") \
    .agg(sum("revenue").alias("total_revenue"))

# Write: Customer Spend
cust=customer_spend.writeStream \
    .option("checkpointLocation", "/mnt/streaming/gold/customer_spend")\
    .outputMode("complete") \
    .format("delta") \
    .toTable("streaming.gold.customer_spend")

# Write: Item Sales
item=item_sales.writeStream \
    .option("checkpointLocation", "/mnt/streaming/gold/item_sales") \
    .outputMode("complete") \
    .format("delta") \
    .toTable("streaming.gold.item_sales")

# Write: Item Revenue
revenue=item_revenue.writeStream \
    .option("checkpointLocation", "/mnt/streaming/gold/item_revenue") \
    .outputMode("complete") \
    .format("delta") \
    .toTable("streaming.gold.item_revenue")

revenue.awaitTermination()
