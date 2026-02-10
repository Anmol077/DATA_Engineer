# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS main.salesDB;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.salesDB.mantable  
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   marks INT
# MAGIC )
# MAGIC USING DELTA  

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO main.salesDB.mantable 
# MAGIC VALUES
# MAGIC (1,'aa',30),
# MAGIC (2,'bb',33),
# MAGIC (3,'cc',35),
# MAGIC (4,'DD',40)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from main.salesDB.mantable;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE main.salesDB.mantable;

# COMMAND ----------

# MAGIC %md
# MAGIC **External Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE main.salesDB.exttable  
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   marks INT 
# MAGIC )
# MAGIC USING DELTA    
# MAGIC LOCATION 'abfss://destination@anmol.dfs.core.windows.net/salesDB/exttable'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO main.salesDB.mantable
# MAGIC VALUES
# MAGIC (1,'aa',30),
# MAGIC (2,'bb',33),
# MAGIC (3,'cc',35),
# MAGIC (4,'DD',40)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.salesDB.mantable;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Tables Functionalities

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO main.salesDB.mantable 
# MAGIC VALUES
# MAGIC (5,'aa',30),
# MAGIC (6,'bb',33),
# MAGIC (7,'cc',35),
# MAGIC (8,'DD',40)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.salesDB.mantable

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM main.salesDB.mantable
# MAGIC WHERE id = 8

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.salesDB.mantable

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA VERSIONING

# COMMAND ----------

# DBTITLE 1,Cell 16
# MAGIC %sql
# MAGIC DESCRIBE HISTORY main.salesDB.mantable;  

# COMMAND ----------

# MAGIC %md
# MAGIC ## TIME TRAVEL

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE main.salesDB.mantable TO VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.salesDB.mantable

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM main.salesDB.mantable; 

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELTA Table Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.salesDB.mantable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.salesDB.mantable

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZORDER BY

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.salesDB.mantable ZORDER BY (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.salesDB.mantable