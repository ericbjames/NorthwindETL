# Databricks notebook source
import org.apache.spark.sql.SparkSession


# COMMAND ----------

# global vars 
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
database_name = username.split("@")[0].replace("-", "_")
# bronze vars 
bronze_product_table_path = f"dbfs:/mnt/dbacademy-users/{username}/bronze/product"
bronze_product_table_name = f"bronze_product"
# silver vars  
silver_product_table_path = f"dbfs:/mnt/dbacademy-users/{username}/silver/product"
silver_product_table_name = f"silver_product"

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS {database_name} 
""")
spark.sql(f"""
USE DATABASE {database_name} 
""")

# COMMAND ----------


val spark = SparkSession.builder()
  .appName("ReadFromRDS")
  .config("spark.driver.extraClassPath", "/path/to/jdbc/driver.jar")
  .getOrCreate()

val jdbcHostname = "myrdsinstance.xxx.us-west-2.rds.amazonaws.com"
val jdbcPort = 3306
val jdbcDatabase = "mydatabase"
val jdbcUsername = "myusername"
val jdbcPassword = "mypassword"

val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

val df = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("user", jdbcUsername)
  .option("password", jdbcPassword)
  .option("dbtable", "mytable")
  .load()

df.show()

