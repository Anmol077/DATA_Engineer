# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# catalog = 'main'
# schema = 'default'
# volume_name = 'volumes'
# spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")

# COMMAND ----------

# files = dbutils.fs.ls(
#     "dbfs:/Volumes/<catalog>/<schema>/<volume>/"
# )
# display(files)

# COMMAND ----------

target_data = [
    (101, "jhon", "Sales", "NY"),
    (102, "mary", "Sales", "CA"),
    (103, "peter", "Marketing", "NY")
]
target_schema = ["Id", "Name", "Dept", "State"]

spark.createDataFrame(
    target_data,
    target_schema
).repartition(1).write.csv(
    # "dbfs:/Volumes/<catalog>/<schema>/<managed or external_volume>/target"
    "/Volumes/main/demo/managed_volume/target_SCD",
    header=True,
    mode="overwrite"
)

# COMMAND ----------

# read file from location 
target_df = spark.read.csv(
    # "dbfs:/Volumes/<catalog>/<schema>/<managed or external_volume>/target"
    "/Volumes/main/demo/managed_volume/target_SCD",
    header=True,
    inferSchema=True
)
display(target_df)

# bit different way to read
# target_df = spark.read \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv("/Volumes/main/demo/managed_volume/target_SCD")
# display(target_df)

# COMMAND ----------

target_scd_df = (target_df
                 .withColumn("is_active", lit(True))
                 .withColumn("start_date", lit("2024-01-01"))
                 .withColumn("end_date", lit("9999-01-01"))
)
display(target_scd_df)

# COMMAND ----------

# source_data_1 = [
#     (101, "jhon", "Sales", "NY"),
#     (102, "mary", "Sales", "CA"),
#     (103, "peter", "Marketing", "NY"),
#     (104, "jack", "Marketing", "NY")
# ]
# source_schema_1= ["Id", "Name", "Dept", "State"]

# spark.createDataFrame(source_data, source_schema)\
#     .coalesce(1)\
#     .write\
#     .mode("overwrite") \
#     .csv("/Volumes/main/demo/managed_volume/source_SCD")


source_data = [
    (101, "jhon", "Sales", "NY"),
    (102, "mary", "Sales", "CA"),
    (103, "peter", "Marketing", "NZ"),
    (104, "jack", "Marketing", "NY")
]
source_schema = ["Id", "Name", "Dept", "State"]

spark.createDataFrame(
    source_data,
    source_schema
).repartition(1).write.csv(
    # "dbfs:/Volumes/<catalog>/<schema>/<managed or external_volume>/target"
    "/Volumes/main/demo/managed_volume/source_SCD",
    header=True,
    mode="overwrite"
)



# COMMAND ----------

source_df = spark.read.csv(
    "/Volumes/main/demo/managed_volume/source_SCD",
    header=True,
    inferSchema=True
)
display(source_df)
# source_df = spark.read.csv(
#     "/Volumes/main/demo/managed_volume/source_SCD",
#     header=True,
#     inferSchema=True
# )
# display(source_df)

# COMMAND ----------

active_target_df = target_scd_df.filter(col("is_active") == True)
display(active_target_df)

# COMMAND ----------

joined_df = source_df.alias("src").join(
    active_target_df.alias("tgt"),
    source_df.Id == active_target_df.Id,
    how = "left"
)
display(joined_df)

# COMMAND ----------

changed_df = joined_df.filter(
    col("tgt.Id").isNotNull()
    & (
        (col("src.Dept") != col("tgt.Dept")) |
        (col("src.State") != col("tgt.State"))
    )
)
display(changed_df)

# COMMAND ----------

expired_df = (
    changed_df
    .select("tgt.*")
    .withColumn("is_Active", lit(False))
    .withColumn("end_date", current_date())
)
display(expired_df
)

# COMMAND ----------

new_versions_df = changed_df.select(
    col("src.id"),
    col("src.name"),
    col("src.dept"),
    col("src.state"),
    lit(True).alias("is_active"),
    current_date().alias("start_date"),
    lit("9999-01-01").alias("end_date")
)
display(new_versions_df)

# COMMAND ----------

new_employees_df = joined_df.filter(
    col("tgt.id").isNull()
).select(
    col("src.id"),
    col("src.name"),
    col("src.dept"),
    col("src.state"),
    lit(True).alias("is_active"),
    current_date().alias("start_date"),
    lit("9999-01-01").alias("end_date")
)
display(new_employees_df)

# COMMAND ----------

# Rename 'Id' in active_target_df to avoid ambiguity
active_target_df_renamed = active_target_df.withColumnRenamed("Id", "target_Id")

# Rename 'Id' in changed_df to match the new name
changed_df_renamed = changed_df.withColumnRenamed("Id", "target_Id")

# Perform the left anti join using the unique column name
unchanged_df = active_target_df_renamed.join(
    changed_df_renamed,
    on="target_Id",
    how="left_anti"
)
display(unchanged_df)

# COMMAND ----------

final_dim_df = (
    expired_df
    .union(unchanged_df)
    .union(new_versions_df)
    .union(new_employees_df)
)
display(final_dim_df)

ascending_df = final_dim_df.orderBy(col("Id").asc())
display(ascending_df)

# COMMAND ----------

ascending_df.repartition(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/main/demo/managed_volume/scd2_final")

display_scd2_final = spark.read.csv(
    "/Volumes/main/demo/managed_volume/scd2_final",
    header=True,
    inferSchema=True
)
display(display_scd2_final)