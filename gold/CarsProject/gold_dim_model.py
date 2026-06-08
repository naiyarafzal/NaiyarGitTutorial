# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag=dbutils.widgets.get('incremental_flag')
print(incremental_flag)


# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimension Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Columns

# COMMAND ----------

df_src=spark.sql('''
            SELECT distinct(Model_ID) as Model_ID, model_category FROM 
            parquet.`abfss://silver@carstorageadls.dfs.core.windows.net/CarSales`
            '''
                )   

# COMMAND ----------

# del df_src
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC - ### dim_model Sink -Initial and Incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink=spark.sql('''
          SELECT dim_model_key, Model_ID, model_category
          FROM cars_catalog.gold.dim_model
          ''')
else:
    df_sink=spark.sql('''
          SELECT 1 as dim_model_key, Model_ID, model_category
          FROM parquet.`abfss://silver@carstorageadls.dfs.core.windows.net/CarSales`
          WHERE 1=0
          ''')    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Model_ID']==df_sink['Model_ID'],'left')\
    .select(df_src['Model_ID'],df_src['model_category'],df_sink['dim_model_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ** df_filter_old **

# COMMAND ----------

df_filter_old=df_filter.filter(col("dim_model_key").isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new=df_filter.filter(col("dim_model_key").isNull()).select(df_src['Model_ID'],df_src['model_category'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch max Surrogate Key from existing table

# COMMAND ----------

if (incremental_flag=='0'):
    max_value=1
else:
    max_value_df=spark.sql("select max(dim_model_key)  from cars_catalog.gold.dim_model")    
    max_value=max_value_df.collect()[0][0]+1 # first [0] return 0th element and second [0] zeroth value

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create surrogate Key Column and Add the max Surrogate Key

# COMMAND ----------

df_filter_new=df_filter_new.withColumn('dim_model_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DF -df_filter_old + df_filter_new

# COMMAND ----------

df_final=df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type -1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl=DeltaTable.forPath(spark,'abfss://gold@carstorageadls.dfs.core.windows.net/dim_model')
    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_key=src.dim_model_key')\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
            .option('path','abfss://gold@carstorageadls.dfs.core.windows.net/dim_model')\
                .saveAsTable('cars_catalog.gold.dim_model')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model

# COMMAND ----------

