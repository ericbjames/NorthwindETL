# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS northwind_group4.silver_order_details_sales;
# MAGIC CREATE TABLE northwind_group4.silver_order_details_sales
# MAGIC USING DELTA
# MAGIC AS
# MAGIC -- SELECT 
# MAGIC -- MD5(concat(CAST(order_id as STRING),CAST(product_id as STRING))) as order_detail_key,
# MAGIC -- order_id,
# MAGIC -- product_id,
# MAGIC -- unit_price,
# MAGIC -- quantity,
# MAGIC -- discount
# MAGIC -- FROM northwind_group4.bronze_order_details
# MAGIC 
# MAGIC SELECT 
# MAGIC   order_id, 
# MAGIC   ROUND((SUM(unit_price * quantity * (1 - discount))),2) AS revenue_per_order
# MAGIC FROM 
# MAGIC   northwind_group4.bronze_order_details
# MAGIC GROUP BY 
# MAGIC   order_id
