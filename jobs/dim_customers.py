# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- Create dim_customers table
# MAGIC DROP TABLE IF EXISTS northwind_group4.dim_customers;
# MAGIC CREATE TABLE northwind_group4.dim_customers
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   MD5(CAST(c.customer_id AS STRING)) as customer_key,
# MAGIC   customer_id,
# MAGIC   c.company_name,
# MAGIC   c.contact_name,
# MAGIC   c.contact_title,
# MAGIC   c.address AS cus_address,
# MAGIC   c.city AS cus_city,
# MAGIC   c.region AS cus_region,
# MAGIC   c.postal_code AS cus_postal_code,
# MAGIC   c.country AS cus_country
# MAGIC FROM northwind_group4.bronze_customers c;
