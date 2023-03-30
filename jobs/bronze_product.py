# Databricks notebook source
# MAGIC %run "./setup_product"

# COMMAND ----------

# dbutils.widgets.text("bronze_product_table_path", bronze_product_table_path)
# dbutils.widgets.text("bronze_product_table_name", bronze_product_table_name)
# dbutils.widgets.text("silver_product_table_path", silver_product_table_path)
# dbutils.widgets.text("silver_product_table_name", silver_product_table_name)

# COMMAND ----------



# COMMAND ----------

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

# dbfs:/user/hive/warehouse/northwind_group4.db


# COMMAND ----------

# MAGIC %md
# MAGIC bronze category

# COMMAND ----------

# bronze categories
df_categories = spark.read.format("delta") \
        .load("dbfs:/user/hive/warehouse/northwind_group4.db/_airbyte_raw_categories")
df_categories.show()


df_categories.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/bronze_categories") \
    .saveAsTable("northwind_group4.bronze_categories")



# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS northwind_group4.bronze_products")


# COMMAND ----------

# bronze products
df_products = spark.read.format("delta") \
        .load("dbfs:/user/hive/warehouse/northwind_group4.db/_airbyte_raw_products")
df_products.show()


df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/bronze_products") \
    .saveAsTable("northwind_group4.bronze_products")



# COMMAND ----------

# bronze suppliers
df_suppliers = spark.read.format("delta") \
        .load("dbfs:/user/hive/warehouse/northwind_group4.db/_airbyte_raw_suppliers")
df_suppliers.show()


df_suppliers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/bronze_suppliers") \
    .saveAsTable("northwind_group4.bronze_suppliers")



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


