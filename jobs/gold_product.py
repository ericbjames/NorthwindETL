# Databricks notebook source
# dimensional modelling, facts and dimension. eg 5 dimensions one fact

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Loading data from Hive") \
        .enableHiveSupport() \
        .getOrCreate()



# northwind_group4._airbyte_raw_categories



# COMMAND ----------

# Read in the silver tables
df_suppliers = spark.table("northwind_group4.silver_suppliers")
df_products = spark.table("northwind_group4.silver_products")
df_categories = spark.table("northwind_group4.silver_categories")

# Join the silver tables
df_joined = df_suppliers.join(df_products, "supplier_id") \
    .join(df_categories, "category_id")

# Aggregate the data to create the gold table
df_gold = df_joined.groupBy("country", "category_name") \
    .agg(
        {"unit_price": "mean", "units_in_stock": "sum"}
    ) \
    .withColumnRenamed("avg(unit_price)", "avg_unit_price") \
    .withColumnRenamed("sum(units_in_stock)", "total_units_in_stock")

# Save the gold table in Delta format
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/gold_table") \
    .saveAsTable("northwind_group4.gold_table")

# Show the gold table
df_gold.show()


# COMMAND ----------



# COMMAND ----------


