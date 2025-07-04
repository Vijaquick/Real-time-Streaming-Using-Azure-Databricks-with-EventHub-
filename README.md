# ⚡ Real-Time Order Processing Pipeline using Azure Event Hub + Databricks + Delta Lake
This project demonstrates an end-to-end real-time streaming ETL pipeline built on Azure Databricks, using Azure Event Hub as the source, and processed through Bronze → Silver → Gold architecture with Structured Streaming and Delta Lake.

| Developed By | LastUpdatedBy | Youtube Channel Name |
|---------------|-------------|-----------------------|
| Vignesan Saravanan   | 04-07-2025   | vijaquick    |

# Architecture
![Real-Time Streaming Architecture](https://raw.githubusercontent.com/malvik01/Real-Time-Streaming-with-Azure-Databricks/main/Azure%20Solution%20Architecture.png)
Azure Event Hub (Order Events) 
          ↓
   🟤 Bronze Layer (Raw staging layer in Delta)
          ↓
   🟦 Silver Layer (Cleaned, flattened, structured tables)
          ↓
   🟨 Gold Layer (Aggregated analytics: customer spend, item sales, revenue)


# Features
- Real-time ingestion from Azure Event Hub

- Bronze Layer: Raw JSON data written to Delta Lake

- Silver Layer: Extracts and normalizes nested JSON into:

Customers

Items

Shipping Address

Payments

🟨 Gold Layer:

Customer total spend

Quantity sold per item

Total revenue per item

- Uses event time + watermarking for late event handling

- Data stored in Delta Lake tables for ACID-compliant updates

# Technologies Used
Azure Databricks (Structured Streaming)

- Azure Event Hub

- PySpark

- Delta Lake

- Azure Data Lake Storage (via /mnt)

