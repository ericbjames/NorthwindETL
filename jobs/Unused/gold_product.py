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

# # Read in the silver tables
# df_suppliers = spark.table("northwind_group4.silver_suppliers")
# df_products = spark.table("northwind_group4.silver_products")
# df_categories = spark.table("northwind_group4.silver_categories")

# # Join the silver tables
# df_joined = df_suppliers.join(df_products, "supplier_id") \
#     .join(df_categories, "category_id")

# # Aggregate the data to create the gold table
# df_gold = df_joined.groupBy("country", "category_name") \
#     .agg(
#         {"unit_price": "mean", "units_in_stock": "sum"}
#     ) \
#     .withColumnRenamed("avg(unit_price)", "avg_unit_price") \
#     .withColumnRenamed("sum(units_in_stock)", "total_units_in_stock")

# # Save the gold table in Delta format
# df_gold.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/gold_table") \
#     .saveAsTable("northwind_group4.gold_table")

# # Show the gold table
# df_gold.show()


# COMMAND ----------

# dbutils.fs.rm("dbfs:/user/hive/warehouse/northwind_group4.db/gold_table", True)


# COMMAND ----------

from pyspark.sql.functions import col
# Read in the silver tables
df_suppliers = spark.table("northwind_group4.silver_suppliers")
df_products = spark.table("northwind_group4.silver_products")
df_categories = spark.table("northwind_group4.silver_categories")


# Join the tables
df_suppliers_products = df_suppliers.join(df_products, "supplier_id") \
    .select(col("supplier_id"), col("company_name"), col("contact_name"), col("contact_title"), col("address"), col("city"), col("region"), col("postal_code"), col("country"), col("phone"), col("fax"), col("homepage"), col("product_id"), col("product_name"), col("category_id"), col("quantity_per_unit"), col("unit_price"), col("units_in_stock"), col("units_on_order"), col("reorder_level"), col("discontinued"))

df_gold = df_suppliers_products.join(df_categories, "category_id") \
    .select(col("product_id"), col("product_name"), col("supplier_id"), col("category_id"), col("category_name"), col("description"), col("quantity_per_unit"), col("unit_price"), col("units_in_stock"), col("units_on_order"), col("reorder_level"), col("discontinued"), col("company_name"), col("contact_name"), col("contact_title"), col("address"), col("city"), col("region"), col("postal_code"), col("country"), col("phone"), col("fax"), col("homepage"))


# Save the gold table in Delta format
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/gold_table") \
    .saveAsTable("northwind_group4.gold_table")

# COMMAND ----------

# Display the resulting joined table
df_gold.show(1)

# COMMAND ----------


