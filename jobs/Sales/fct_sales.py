# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS northwind_group4.fct_sales;
# MAGIC 
# MAGIC CREATE TABLE northwind_group4.fct_sales
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   MD5(CONCAT(CAST(dd.date_key AS STRING), CAST(dc.customer_key AS STRING), CAST(do.order_key AS STRING))) AS sales_key,
# MAGIC   dd.date_key,
# MAGIC   dc.customer_key,
# MAGIC   do.order_key,
# MAGIC   dod.revenue_per_order
# MAGIC FROM northwind_group4.dim_orders do
# MAGIC JOIN northwind_group4.dim_date dd ON do.order_id = dd.order_id
# MAGIC JOIN northwind_group4.dim_customers dc ON dc.customer_id = do.customer_id
# MAGIC JOIN northwind_group4.silver_order_details_sales dod ON dod.order_id  = do.order_id
