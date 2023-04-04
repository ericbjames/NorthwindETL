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

# MAGIC %md
# MAGIC # Reading in and cleaning the data

# COMMAND ----------

df_categories = spark.table("northwind_group4.bronze_categories")
df_categories = df_categories.drop(*[c for c in df_categories.columns if c.startswith('_airbyte_')])
df_categories.show()

# COMMAND ----------

df_order_details = spark.table("northwind_group4.bronze_order_details")
df_order_details.show()


# COMMAND ----------



# COMMAND ----------

df_orders= spark.table("northwind_group4.bronze_orders")
df_orders=df_orders.drop("product_id", "unit_price", "quantity", "discount")
df_orders.show()


# COMMAND ----------

df_orders_merged = df_orders.join(df_order_details, on='order_id')
df_orders_merged.show()

# COMMAND ----------

df_products = spark.table("northwind_group4.bronze_products")
df_products = df_products.drop(*[c for c in df_products.columns if c.startswith('_airbyte_')])

df_products.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Do some kind of aggregation and dimensional modelling here (Done by eric) - have to do data quality testing as well (great expectations)

# COMMAND ----------



# COMMAND ----------

# MAGIC %pip install great_expectations

# COMMAND ----------


from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.master("local").appName("GE Databricks Example").getOrCreate()

# Load your tables as Spark DataFrames
bronze_orders_df = spark.read.table("northwind_group4.bronze_orders")
bronze_order_details_df = spark.read.table("northwind_group4.bronze_order_details")
bronze_customers_df = spark.read.table("northwind_group4.bronze_customers")
dim_customers_df = spark.read.table("northwind_group4.dim_customers")
fct_sales_df = spark.read.table("northwind_group4.fct_sales")

# Remove rows with null values for specified columns
bronze_orders_df_cleaned = bronze_orders_df.na.drop(subset=["order_id"])
bronze_order_details_df_cleaned = bronze_order_details_df.na.drop(subset=["order_id"])
bronze_customers_df_cleaned = bronze_customers_df.na.drop(subset=["customer_id"])
dim_customers_df_cleaned = dim_customers_df.na.drop(subset=["customer_key"])
fct_sales_df_cleaned = fct_sales_df.na.drop(subset=["sales_key"])

# Overwrite the original tables with the cleaned DataFrames
bronze_orders_df_cleaned.write.mode("overwrite").saveAsTable("northwind_group4.bronze_orders")
bronze_order_details_df_cleaned.write.mode("overwrite").saveAsTable("northwind_group4.bronze_order_details")
bronze_customers_df_cleaned.write.mode("overwrite").saveAsTable("northwind_group4.bronze_customers")
dim_customers_df_cleaned.write.mode("overwrite").saveAsTable("northwind_group4.dim_customers")
fct_sales_df_cleaned.write.mode("overwrite").saveAsTable("northwind_group4.fct_sales")

# Wrap the cleaned DataFrames with Great Expectations SparkDFDataset
bronze_orders_ge = SparkDFDataset(bronze_orders_df_cleaned)
bronze_order_details_ge = SparkDFDataset(bronze_order_details_df_cleaned)
bronze_customers_ge = SparkDFDataset(bronze_customers_df_cleaned)
dim_customers_ge = SparkDFDataset(dim_customers_df_cleaned)
fct_sales_ge = SparkDFDataset(fct_sales_df_cleaned)




# COMMAND ----------

# Define all the expectations


bronze_customers_ge.expect_column_values_to_match_regex("customer_id", "^[A-Za-z]+$")

bronze_orders_ge.expect_column_values_to_not_be_null("order_id")
bronze_orders_ge.expect_column_values_to_be_unique("order_id")

bronze_order_details_ge.expect_column_values_to_not_be_null("order_id")

bronze_customers_ge.expect_column_values_to_not_be_null("customer_id")
bronze_customers_ge.expect_column_values_to_be_unique("customer_id")

dim_customers_ge.expect_column_values_to_not_be_null("customer_key")
dim_customers_ge.expect_column_values_to_be_unique("customer_key")


fct_sales_ge.expect_column_values_to_not_be_null("sales_key")
fct_sales_ge.expect_column_values_to_be_unique("sales_key")



# COMMAND ----------

from pprint import pprint

# Validate tables against their Expectation
bronze_customers_validation_result = bronze_customers_ge.validate()

datasets = {
    "bronze_orders": bronze_orders_ge,
    "bronze_order_details": bronze_order_details_ge,
    "bronze_customers": bronze_customers_ge,
    "dim_customers": dim_customers_ge,
    "fct_sales": fct_sales_ge
}

for dataset_name, dataset in datasets.items():
    validation_result = dataset.validate()
    if validation_result["success"]:
        print(f"Validation for {dataset_name} was successful!")
    else:
        print(f"Validation for {dataset_name} failed. Failed expectations:")
        for result in validation_result["results"]:
            if not result["success"]:
                pprint(result)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # saving them into silver tables

# COMMAND ----------

# # Save the transformed dataframe in Parquet format
# df_categories_transformed.write \
#     .format("parquet") \
#     .mode("overwrite") \
#     .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/silver_categories") \
#     .saveAsTable("northwind_group4.silver_categories")




# # Save the transformed dataframe in Parquet format
# df_products_transformed.write \
#     .format("parquet") \
#     .mode("overwrite") \
#     .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/silver_products") \
#     .saveAsTable("northwind_group4.silver_products")



# # Save the transformed dataframe in Parquet format
# df_suppliers_transformed.write \
#     .format("parquet") \
#     .mode("overwrite") \
#     .option("path", "dbfs:/user/hive/warehouse/northwind_group4.db/silver_suppliers") \
#     .saveAsTable("northwind_group4.silver_suppliers")




# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


