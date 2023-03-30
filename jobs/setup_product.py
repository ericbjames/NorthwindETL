# Databricks notebook source
# global vars 
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
database_name = username.split("@")[0].replace("-", "_")
# bronze vars 
# bronze_product_table_path = f"dbfs:/mnt/dbacademy-users/{username}/bronze/product"
bronze_product_table_path = f"dbfs:/user/hive/warehouse/northwind_group4.db/bronze/product"
bronze_product_table_name = f"bronze_product"
# silver vars  
# silver_product_table_path = f"dbfs:/mnt/dbacademy-users/{username}/silver/product"
bronze_product_table_path = f"dbfs:/user/hive/warehouse/northwind_group4.db/silver/product"
silver_product_table_name = f"silver_product"

# COMMAND ----------


