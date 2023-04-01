# Databricks notebook source
#%run "./setup_product"

# COMMAND ----------

# dbutils.widgets.text("bronze_product_table_path", bronze_product_table_path)
# dbutils.widgets.text("bronze_product_table_name", bronze_product_table_name)
# dbutils.widgets.text("silver_product_table_path", silver_product_table_path)
# dbutils.widgets.text("silver_product_table_name", silver_product_table_name)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Loading data from Hive") \
        .enableHiveSupport() \
        .getOrCreate()



# northwind_group4._airbyte_raw_categories



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

df_categories = spark.table("northwind_group4.bronze_categories")

# Define the schema for the JSON column
schema = StructType([
    StructField("category_id", IntegerType()),
    StructField("category_name", StringType()),
    StructField("description", StringType())
])

# Convert the JSON column to individual columns and select relevant columns
df_categories_transformed = df_categories.select(from_json(col("_airbyte_data"), schema).alias("data")).select("data.*")

# Save the transformed dataframe in Parquet format
df_categories_transformed.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/silver_categories") \
    .saveAsTable("northwind_group4.silver_categories")

df_categories_transformed.show()


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

df_products = spark.table("northwind_group4.bronze_products")

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

# Save the transformed dataframe in Parquet format
df_products_transformed.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/silver_products") \
    .saveAsTable("northwind_group4.silver_products")

df_products_transformed.show()


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


df_suppliers = spark.table("northwind_group4.bronze_suppliers")


# Define the schema for the JSON column
schema = StructType([
    StructField("supplier_id", IntegerType()),
    StructField("company_name", StringType()),
    StructField("contact_name", StringType()),
    StructField("contact_title", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("region", StringType()),
    StructField("postal_code", StringType()),
    StructField("country", StringType()),
    StructField("phone", StringType()),
    StructField("fax", StringType()),
    StructField("homepage", StringType())
])

# Convert the JSON column to individual columns and select relevant columns
df_suppliers_transformed = df_suppliers.select(from_json(col("_airbyte_data"), schema).alias("data")).select("data.*")

# Save the transformed dataframe in Parquet format
df_suppliers_transformed.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/silver_suppliers") \
    .saveAsTable("northwind_group4.silver_suppliers")


# COMMAND ----------

# MAGIC   %pip install great-expectations
