# Databricks notebook source
# MAGIC %md
# MAGIC DATA READING

# COMMAND ----------

df=spark.read.format('parquet')\
    .option('inferSchema',True)\
        .load('abfss://bronze@carstorageadls.dfs.core.windows.net/raw_data')


# COMMAND ----------

# MAGIC %md
# MAGIC # Display Data

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding a new column

# COMMAND ----------

df=df.withColumn('Model_Category',split(col("Model_ID"),'-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Changing the data type of a colum and printing the schema

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Revenue Per Unit

# COMMAND ----------

df=df.withColumn('RevPerUnit',col('Revenue')/col("Units_Sold"))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC (group by )

# COMMAND ----------

df.groupBy("Year","BranchName").agg(sum("Units_Sold").alias('Total_Units_Sold'))\
    .sort("Year",'Total_Units_Sold',ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # writing data 

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
        .option('path','abfss://silver@carstorageadls.dfs.core.windows.net/CarSales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # 

# COMMAND ----------

# MAGIC %md
# MAGIC # Fetching data from silver layer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM parquet.`abfss://silver@carstorageadls.dfs.core.windows.net/CarSales`

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

