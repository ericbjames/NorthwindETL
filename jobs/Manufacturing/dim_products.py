# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- Create dim_product table
# MAGIC DROP TABLE IF EXISTS northwind_group4.dim_products;
# MAGIC CREATE TABLE northwind_group4.dim_products
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC   MD5(CAST(p.product_id AS STRING)) as product_key,
# MAGIC   product_id,
# MAGIC   p.product_name as product_name,
# MAGIC   p.unit_price as unit_price,
# MAGIC   p.quantity_per_unit as quantity_per_unit,
# MAGIC   p.units_in_stock as units_in_stock,
# MAGIC   p.units_on_order as units_on_order,
# MAGIC   p.reorder_level as reorder_level,
# MAGIC   p.discontinued as discontinued,
# MAGIC   c.category_id as category_id,
# MAGIC   c.category_name as category_name
# MAGIC FROM northwind_group4.bronze_products p
# MAGIC JOIN northwind_group4.bronze_categories c ON p.category_id = c.category_id
