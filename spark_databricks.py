# Databricks notebook source
from pyspark.sql.types import *

person_list = [
    ("Berry", "", "Allen", 1, "M"),
    ("Oliver", "Queen", "", 2, "M"),
    ("Robert", "", "Williams", 3, "M"),
    ("Tony", "", "Stark", 4, "F"),
    ("Rajiv", "Mary", "Kumar", 5, "F")
]

person_schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("gender", StringType(), True)
])

personDf = spark.createDataFrame(data=person_list, schema=person_schema)
display(personDf)
personDf.printSchema()

# COMMAND ----------

deptDf = spark.read.option("header", True).csv("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/employee.csv")
display(deptDf)
deptDf.printSchema()

deptDf2 = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/employee.csv")
display(deptDf2)
deptDf2.printSchema()

# COMMAND ----------

empDf = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/employee.csv")
display(empDf)
empDf.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

display(empDf.select("EMPLOYEE_ID", "FIRST_NAME"))
display(empDf.select(col("EMPLOYEE_ID").alias("EMP_ID"), col("FIRST_NAME").alias("F_NAME")))
display(empDf.select("EMPLOYEE_ID", "FIRST_NAME", "SALARY").withColumn("NEW_SALARY", col("SALARY") + 1000))
display(empDf.withColumn("NEW_SALARY", col("SALARY") + 1000).select("EMPLOYEE_ID", "FIRST_NAME", "NEW_SALARY"))
display(empDf.withColumn("SALARY", col("SALARY") - 1000).select("EMPLOYEE_ID", "FIRST_NAME", "SALARY"))
display(empDf.withColumnRenamed("SALARY", "EMP_SALARY"))
display(empDf.drop("COMMISSION_PCT"))

# COMMAND ----------

display(empDf.filter(col("SALARY") < 5000))
display(empDf.filter((col("DEPARTMENT_ID") == 50) & (col("SALARY") < 5000)).select("EMPLOYEE_ID", "FIRST_NAME", "SALARY", "DEPARTMENT_ID"))
display(empDf.filter("DEPARTMENT_ID <> 50").select("EMPLOYEE_ID", "FIRST_NAME", "SALARY", "DEPARTMENT_ID"))
display(empDf.filter("DEPARTMENT_ID == 50 and SALARY < 5000").select("EMPLOYEE_ID", "FIRST_NAME", "SALARY", "DEPARTMENT_ID"))

# COMMAND ----------

display(empDf.distinct())
display(empDf.dropDuplicates())
display(empDf.dropDuplicates(["DEPARTMENT_ID", "HIRE_DATE"]))
display(empDf.dropDuplicates(["DEPARTMENT_ID", "HIRE_DATE"]).select("EMPLOYEE_ID", "HIRE_DATE", "DEPARTMENT_ID"))

# COMMAND ----------

from pyspark.sql.functions import count, max, min, avg, sum

empDf.count()
display(empDf.select(count("salary").alias("total_count")))
display(empDf.select(max("salary").alias("max_salary")))
display(empDf.select(min("salary").alias("min_salary")))
display(empDf.select(avg("salary").alias("avg_salary")))
display(empDf.select(sum("salary").alias("sum_salary")))

# COMMAND ----------

display(empDf.groupBy("DEPARTMENT_ID").sum("SALARY"))
display(empDf.groupBy("DEPARTMENT_ID").max("SALARY"))
display(empDf.groupBy("DEPARTMENT_ID").min("SALARY"))
display(empDf.groupBy("DEPARTMENT_ID", "JOB_ID").sum("SALARY", "EMPLOYEE_ID"))
display(empDf.groupBy("DEPARTMENT_ID").agg(
    sum("SALARY").alias("SUM_SALARY"),
    max("SALARY").alias("MAX_SALARY"),
    min("SALARY").alias("MIN_SALARY"),
    avg("SALARY").alias("AVG_SALARY")
))

# COMMAND ----------

display(empDf.groupBy("DEPARTMENT_ID").agg(
    sum("SALARY").alias("SUM_SALARY"),
    max("SALARY").alias("MAX_SALARY"),
    min("SALARY").alias("MIN_SALARY"),
    avg("SALARY").alias("AVG_SALARY")
).where(col("MAX_SALARY") >= 10000))

# COMMAND ----------

from pyspark.sql.functions import when

df = empDf.withColumn(
    "EMP_GRADE",
    when(col("SALARY") > 15000, "A")
    .when((col("SALARY") >= 10000) & (col("SALARY") < 15000), "B")
    .otherwise("C")
)
display(df.select("EMPLOYEE_ID", "SALARY", "EMP_GRADE"))

# COMMAND ----------

empDf.createOrReplaceTempView("employee")
display(spark.sql("SELECT * FROM employee LIMIT 5"))
display(spark.sql("SELECT employee_id, salary FROM employee"))
display(spark.sql("SELECT department_id, SUM(salary) AS sum_salary FROM employee GROUP BY department_id"))
display(spark.sql("""
    SELECT employee_id, department_id,
    RANK() OVER(PARTITION BY department_id ORDER BY salary DESC) AS rank_salary
    FROM employee
"""))

# COMMAND ----------

# DBTITLE 1,Untitled
display(empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner"))
display(empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_ID))
display(empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "left").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_ID))
display(empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "right").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_ID))
display(empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "fullouter").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_ID))

# COMMAND ----------

display(deptDf.filter(deptDf.MANAGER_ID != "-"))

# COMMAND ----------

# DBTITLE 1,Cell 14
from pyspark.sql.functions import col

emp1 = empDf.alias("emp1")
emp2 = empDf.alias("emp2")
# Filter emp1 to only rows where manager_id is numeric
emp1_filtered = emp1.filter(col("MANAGER_ID").rlike("^[0-9]+$"))
display(
    emp1_filtered.join(emp2, col("emp1.MANAGER_ID") == col("emp2.EMPLOYEE_ID"), "inner")
    .select(col("emp1.MANAGER_ID"), col("emp2.FIRST_NAME"), col("emp2.LAST_NAME"))
    .dropDuplicates()
)

# COMMAND ----------

# DBTITLE 1,Cell 15
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

location_data = [(1700, "INDIA"), (1800, "USA")]
location_schema = StructType([
    StructField("LOCATION_ID", IntegerType(), True),
    StructField("LOCATION_NAME", StringType(), True)
])
locDf = spark.createDataFrame(data=location_data, schema=location_schema)

# Join empDf and deptDf on DEPARTMENT_ID, then join locDf using a constant LOCATION_ID value
joinedDf = empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner")
joinedDf = joinedDf.join(locDf, locDf.LOCATION_ID == 1700, "inner")
display(joinedDf.select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, locDf.LOCATION_NAME))

# COMMAND ----------

from pyspark.sql.functions import udf

def upperCase(in_str):
    return in_str.upper() if in_str else None

upperCaseUDF = udf(upperCase, StringType())
display(
    empDf.select(
        col("EMPLOYEE_ID"),
        col("FIRST_NAME"),
        col("LAST_NAME"),
        upperCaseUDF(col("FIRST_NAME")).alias("FIRST_NAME_UPPER"),
        upperCaseUDF(col("LAST_NAME")).alias("LAST_NAME_UPPER")
    )
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, sum as sum_

windowSpec = Window.partitionBy("DEPARTMENT_ID").orderBy(col("SALARY").desc())
display(empDf.withColumn("salary_rank", rank().over(windowSpec)).select("DEPARTMENT_ID", "SALARY", "salary_rank"))

windowSpecSum = Window.partitionBy("DEPARTMENT_ID")
display(empDf.withColumn("SUM", sum_("SALARY").over(windowSpecSum)).select("DEPARTMENT_ID", "SALARY", "SUM"))

# COMMAND ----------

# DBTITLE 1,Cell 18
resultDf = empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID)
resultDf.write.mode("overwrite").partitionBy("DEPARTMENT_ID").option("header", True).format("csv").save("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/modified_employee.csv")

# COMMAND ----------

# DBTITLE 1,Cell 19
resultDf = empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID)
resultDf.write.mode("ignore").partitionBy("DEPARTMENT_ID").option("header", True).format("csv").save("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/new_employee.csv")

# COMMAND ----------

resultDf = empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_NAME)
resultDf.write.mode("errorifexists").partitionBy("DEPARTMENT_NAME").option("header", True).format("csv").save("/Volumes/intellibi_catalog/intellibi_schema/intellibi_data/Spark-data/result")

# COMMAND ----------

# DBTITLE 1,Cell 21
resultDf = empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID)
resultDf.write.mode("overwrite").partitionBy("DEPARTMENT_ID").option("header", True).format("parquet").save("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/result_parquet")

# COMMAND ----------

parquetDF = spark.read.option("header", True).option("inferSchema", True).parquet("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/result_parquet/DEPARTMENT_ID=10/part-00000-tid-1966850511824648557-c9888bf0-efdf-4c6e-aebb-94d3089cd63d-1741-1.c000.snappy.parquet")
display(parquetDF)
parquetDF.printSchema()

# COMMAND ----------

parquetDF = spark.read.option("header", True).option("inferSchema", True).parquet("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/result_parquet/DEPARTMENT_ID=10/part-00000-tid-1966850511824648557-c9888bf0-efdf-4c6e-aebb-94d3089cd63d-1741-1.c000.snappy.parquet")
display(parquetDF)
parquetDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 24
resultDf = empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID)
resultDf.write.mode("overwrite").partitionBy("DEPARTMENT_ID").option("header", True).format("delta").save("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/result_delta")

# COMMAND ----------

# DBTITLE 1,Cell 24
resultDf = empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID)
resultDf.write.mode("overwrite").partitionBy("DEPARTMENT_ID").option("header", True).format("delta").save("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/result_delta")

# COMMAND ----------

