# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- Create dim_suppliers table
# MAGIC DROP TABLE IF EXISTS northwind_group4.dim_suppliers;
# MAGIC CREATE TABLE northwind_group4.dim_suppliers
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC   MD5(CAST(s.supplier_id AS STRING)) as supplier_key,
# MAGIC   s.company_name,
# MAGIC   s.address as sup_address,
# MAGIC   s.city as sup_city,
# MAGIC   s.region as sup_region,
# MAGIC   s.postal_code as sup_postal_code,
# MAGIC   s.country as sup_country
# MAGIC FROM northwind_group4.bronze_suppliers s;
