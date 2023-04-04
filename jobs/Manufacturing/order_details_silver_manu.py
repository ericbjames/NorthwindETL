# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS northwind_group4.order_details_silver_manu;
# MAGIC CREATE TABLE northwind_group4.order_details_silver_manu
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   ROUND((SUM(unit_price * quantity * (1 - discount))),2) AS product_sold
# MAGIC FROM 
# MAGIC   northwind_group4.bronze_order_details
# MAGIC GROUP BY 
# MAGIC   product_id
