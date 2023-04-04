# Databricks notebook source
# MAGIC %sql
# MAGIC -- Drop the dim_date table if it exists
# MAGIC DROP TABLE IF EXISTS northwind_group4.dim_date;
# MAGIC 
# MAGIC -- Create the dim_date table
# MAGIC CREATE TABLE northwind_group4.dim_date
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC   MD5(CONCAT(CAST(order_date AS STRING), CAST(required_date AS STRING), CAST(shipped_date AS STRING), CAST(order_id AS STRING))) AS date_key,
# MAGIC   order_id,
# MAGIC   CAST(order_date AS DATE) AS order_date,
# MAGIC   CAST(required_date AS DATE) AS required_date,
# MAGIC   CAST(shipped_date AS DATE) AS shipped_date,
# MAGIC   DATEDIFF(shipped_date, order_date) AS days_to_ship
# MAGIC FROM (
# MAGIC   SELECT order_date, required_date, shipped_date, order_id FROM northwind_group4.bronze_orders
# MAGIC   WHERE order_id IS NOT NULL
# MAGIC ) AS all_dates
# MAGIC GROUP BY order_date, required_date, shipped_date, order_id;
