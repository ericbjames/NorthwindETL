# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- Create dim_product table
# MAGIC DROP TABLE IF EXISTS northwind_group4.fct_manufacturing;
# MAGIC CREATE TABLE northwind_group4.fct_manufacturing
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC   MD5(Concat(CAST(p.product_id AS STRING),CAST(p.supplier_id AS STRING)) as manu_key,
# MAGIC   p.unit_price as unit_price,
# MAGIC   p.quantity_per_unit as quantity_per_unit,
# MAGIC   p.units_in_stock as units_in_stock,
# MAGIC   p.units_on_order as units_on_order,
# MAGIC   p.reorder_level as reorder_level,
# MAGIC   p.discontinued as discontinued,
# MAGIC   c.category_id as category_id,
# MAGIC   c.category_name as category_name 
# MAGIC   od.product_sold as product_sold
# MAGIC FROM northwind_group4.order_details_silver_manu as od
# MAGIC JOIN northwind_group4.dim_products p ON p.product_id = od.product_id
