# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Loading data from Hive") \
        .enableHiveSupport() \
        .getOrCreate()



# northwind_group4._airbyte_raw_categories



# COMMAND ----------

# MAGIC %md
# MAGIC # read file from delta lake

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,TimestampType


# COMMAND ----------



# Read the data from the Delta table
df_categories = spark.read.format("delta") \
        .load("dbfs:/user/hive/warehouse/northwind_group4.db/_airbyte_raw_categories")

# Define the schema for the JSON column
schema = StructType([
    StructField("category_id", IntegerType()),
    StructField("category_name", StringType()),
    StructField("description", StringType())
])

# Convert the JSON column to individual columns and select relevant columns
df_categories_transformed = df_categories.select(from_json(col("_airbyte_data"), schema).alias("data")).select("data.*")

# Enable schema migration and save the transformed dataframe in Delta format
df_categories_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/bronze_categories") \
    .saveAsTable("northwind_group4.bronze_categories")


# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS northwind_group4.bronzeproduct")


# COMMAND ----------


# Read the data from the Delta table
df_products = spark.read.format("delta") \
        .load("dbfs:/user/hive/warehouse/northwind_group4.db/_airbyte_raw_products")

# Define the schema for the JSON column
schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("supplier_id", IntegerType()),
    StructField("category_id", IntegerType()),
    StructField("quantity_per_unit", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("units_in_stock", IntegerType()),
    StructField("units_on_order", IntegerType()),
    StructField("reorder_level", IntegerType()),
    StructField("discontinued", IntegerType())
])

# Convert the JSON column to individual columns and select relevant columns
df_products_transformed = df_products.select(from_json(col("_airbyte_data"), schema).alias("data")).select("data.*")

# Enable schema migration and save the transformed dataframe in Delta format
df_products_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/bronze_products") \
    .saveAsTable("northwind_group4.bronze_products")


# COMMAND ----------

df_products_transformed.show()

# COMMAND ----------

df_orders = spark.read.format("delta") \
        .load("dbfs:/user/hive/warehouse/northwind_group4.db/_airbyte_raw_order_details")
df_orders.show()

# COMMAND ----------


# Read the data from the Delta table
df_order_details = spark.read.format("delta") \
        .load("dbfs:/user/hive/warehouse/northwind_group4.db/_airbyte_raw_order_details")

# Define the schema for the JSON column
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("discount", DoubleType())
])

# Convert the JSON column to individual columns and select relevant columns
df_order_details_transformed = df_order_details.select(from_json(col("_airbyte_data"), schema).alias("data")).select("data.*")

# Enable schema migration and save the transformed dataframe in Delta format
df_order_details_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/bronze_order_details") \
    .saveAsTable("northwind_group4.bronze_order_details")


# COMMAND ----------



# Read the data from the Delta table
df_orders = spark.read.format("delta") \
        .load("dbfs:/user/hive/warehouse/northwind_group4.db/_airbyte_raw_orders")

# Define the schema for the JSON column
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", StringType()),
    StructField("employee_id", IntegerType()),
    StructField("order_date", TimestampType()),
    StructField("required_date", TimestampType()),
    StructField("shipped_date", TimestampType()),
    StructField("ship_via", IntegerType()),
    StructField("freight", StringType()),
    StructField("ship_name", StringType()),
    StructField("ship_address", StringType()),
    StructField("ship_city", StringType()),
    StructField("ship_region", StringType()),
    StructField("ship_postal_code", StringType()),
    StructField("ship_country", StringType())
])

# Convert the JSON column to individual columns and select relevant columns
df_orders_transformed = df_orders.select(from_json(col("_airbyte_data"), schema).alias("data")).select("data.*")

# Enable schema migration and save the transformed dataframe in Delta format
df_orders_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/bronze_orders") \
    .saveAsTable("northwind_group4.bronze_orders")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


