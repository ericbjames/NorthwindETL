# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- Create dim_orders table
# MAGIC DROP TABLE IF EXISTS northwind_group4.dim_orders;
# MAGIC CREATE TABLE northwind_group4.dim_orders
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   MD5(CAST(o.order_id AS STRING)) as order_key,
# MAGIC   o.order_id,
# MAGIC   o.customer_id,
# MAGIC   s.company_name as shipping_company_name,
# MAGIC   o.freight as shipping_cost,
# MAGIC   o.ship_name,
# MAGIC   o.ship_address,
# MAGIC   o.ship_city,
# MAGIC   o.ship_region,
# MAGIC   o.ship_postal_code,
# MAGIC   o.ship_country
# MAGIC FROM northwind_group4.bronze_orders as o
# MAGIC JOIN northwind_group4.bronze_shippers as s
# MAGIC   ON s.shipper_id = o.ship_via;
