# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS main.scd_demo;
# MAGIC use catalog main;
# MAGIC USE schema scd_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_customer (
# MAGIC   customer_sk   BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   customer_id   INT,
# MAGIC   customer_name STRING,
# MAGIC   city          STRING,
# MAGIC   segment       STRING,
# MAGIC   start_date    DATE,
# MAGIC   end_date      DATE,
# MAGIC   is_current    BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE stg_customer (
# MAGIC   customer_id   INT,
# MAGIC   customer_name STRING,
# MAGIC   city          STRING,
# MAGIC   segment       STRING
# MAGIC  )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dim_customer
# MAGIC (customer_id, customer_name, city, segment, start_date, end_date, is_current)
# MAGIC VALUES
# MAGIC (101, 'Aman',   'Delhi',     'Retail', '2023-01-01', '9999-12-31', true),
# MAGIC (102, 'Rohit',  'Mumbai',    'Retail', '2023-01-01', '9999-12-31', true),
# MAGIC (103, 'Neha',   'Pune',      'SMB',    '2023-01-01', '9999-12-31', true),
# MAGIC (104, 'Ankit',  'Bangalore', 'SMB',    '2023-01-01', '9999-12-31', true),
# MAGIC (105, 'Priya',  'Chennai',   'Retail', '2023-01-01', '9999-12-31', true),
# MAGIC (106, 'Rahul',  'Hyderabad', 'SMB',    '2023-01-01', '9999-12-31', true),
# MAGIC (107, 'Kiran',  'Kolkata',   'Retail', '2023-01-01', '9999-12-31', true),
# MAGIC (108, 'Sonal',  'Noida',     'SMB',    '2023-01-01', '9999-12-31', true),
# MAGIC (109, 'Vikas',  'Jaipur',    'Retail', '2023-01-01', '9999-12-31', true),
# MAGIC (110, 'Meera',  'Indore',    'SMB',    '2023-01-01', '9999-12-31', true);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO stg_customer VALUES
# MAGIC  (101, 'Aman',   'Bangalore', 'Retail'),   -- City changed
# MAGIC  (102, 'Rohit',  'Mumbai',    'Enterprise'),-- Segment changed
# MAGIC  (103, 'Neha',   'Pune',      'SMB'),      -- No change
# MAGIC  (104, 'Ankit',  'Delhi',     'SMB'),      -- City changed
# MAGIC  (105, 'Priya',  'Chennai',   'Retail'),   -- No change
# MAGIC  (111, 'Nitin',  'Nagpur',    'Retail'),   -- New
# MAGIC  (112, 'Pooja',  'Surat',     'SMB'),      -- New
# MAGIC  (113, 'Arjun',  'Ahmedabad','Enterprise'),-- New
# MAGIC  (114, 'Riya',   'Bhopal',    'Retail'),   -- New
# MAGIC  (115, 'Kabir',  'Udaipur',   'SMB');      -- New

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg_customer;

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE OR REPLACE TEMP VIEW scd2_source AS
# MAGIC
# MAGIC -- ROW TO EXPIRE EXISTING RECORD
# MAGIC  SELECT
# MAGIC      tgt.customer_id       AS merge_customer_id,
# MAGIC      tgt.customer_id       AS customer_id,
# MAGIC     tgt.customer_name,
# MAGIC   tgt.city,
# MAGIC      tgt.segment,
# MAGIC     false                 AS is_new
# MAGIC  FROM dim_customer tgt
# MAGIC  JOIN stg_customer src
# MAGIC    ON tgt.customer_id = src.customer_id
# MAGIC   AND tgt.is_current = true
# MAGIC  WHERE tgt.customer_name <> src.customer_name
# MAGIC     OR tgt.city <> src.city
# MAGIC    OR tgt.segment <> src.segment
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC --  ROW TO INSERT NEW CURRENT RECORD
# MAGIC  SELECT
# MAGIC     src.customer_id      AS merge_customer_id, -- 🔥 KEY TRICK
# MAGIC     src.customer_id       AS customer_id,
# MAGIC    src.customer_name,
# MAGIC      src.city,
# MAGIC    src.segment,
# MAGIC     true                  AS is_new
# MAGIC  FROM stg_customer src
# MAGIC  LEFT JOIN dim_customer tgt
# MAGIC   ON src.customer_id = tgt.customer_id
# MAGIC   AND tgt.is_current = true
# MAGIC WHERE tgt.customer_id IS NULL
# MAGIC    OR tgt.customer_name <> src.customer_name
# MAGIC     OR tgt.city <> src.city
# MAGIC     OR tgt.segment <> src.segment;
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC  MERGE INTO dim_customer tgt
# MAGIC  USING scd2_source src
# MAGIC ON tgt.customer_id = src.merge_customer_id
# MAGIC  AND tgt.is_current = true
# MAGIC
# MAGIC  -- EXPIRE OLD
# MAGIC  WHEN MATCHED AND src.is_new = false THEN
# MAGIC    UPDATE SET
# MAGIC      tgt.end_date = current_date() - 1,
# MAGIC      tgt.is_current = false
# MAGIC
# MAGIC -- INSERT NEW
# MAGIC  WHEN NOT MATCHED AND src.is_new = true THEN
# MAGIC    INSERT (
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     city,
# MAGIC      segment,
# MAGIC     start_date,
# MAGIC    end_date,
# MAGIC     is_current
# MAGIC    )
# MAGIC   VALUES (
# MAGIC      src.customer_id,
# MAGIC      src.customer_name,
# MAGIC      src.city,
# MAGIC     src.segment,
# MAGIC      current_date(),
# MAGIC      DATE '9999-12-31',
# MAGIC     true
# MAGIC    );

# COMMAND ----------

# MAGIC  %sql
# MAGIC SELECT *
# MAGIC FROM dim_customer
# MAGIC ORDER BY customer_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dim_customer
# MAGIC WHERE is_current = true
# MAGIC ORDER BY customer_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC  SELECT *
# MAGIC  FROM dim_customer
# MAGIC  WHERE customer_id IN (101, 102, 104)
# MAGIC  ORDER BY customer_id, start_date;