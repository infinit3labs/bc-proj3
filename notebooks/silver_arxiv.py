# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleanse bronze Arxiv article data into silver table
# MAGIC This notebook takes the latest batch of Arxiv research paper data, assigns appropriate data types, and incrementally loads articles updated after the prior run into a silver delta table.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text is_fresh_load default '0'

# COMMAND ----------

table_name = 'arxiv'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code to revert to clean state if fresh load

# COMMAND ----------

if dbutils.widgets.get('is_fresh_load') == '1':
    spark.sql(f"drop table if exists main.silver.{table_name}")
    spark.sql(f"drop table if exists main.silver.watermark_{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for watermark value

# COMMAND ----------

# Does silver table exist?
current_silver_tables = [t.name for t in spark.catalog.listTables('main.silver')]
current_silver_tables

# COMMAND ----------

watermark_date = None
if f'watermark_{table_name}' not in current_silver_tables and table_name not in current_silver_tables:
    watermark_date = '1970-01-01'
elif f'watermark_{table_name}' in current_silver_tables and table_name in current_silver_tables:
    watermark_date = spark.read.table(f'main.silver.watermark_{table_name}').collect()[0]['watermark_date']
else:
    dbutils.notebook.exit('Preconditions not met - either both table and watermark exists, or neither')
watermark_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

df = spark.table(f'main.bronze.{table_name}')

# COMMAND ----------

# Get row count prior to transformation - no rows should be removed during this process
pre_rowcount = df.count()
pre_rowcount

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView('source')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix data types

# COMMAND ----------

source_df = spark.sql("""
select split(split(id, '/')[4], 'v')[0]::string as id,
       split(split(id, '/')[4], 'v')[1]::string as version,
       id::string as link,  
       summary::string,
       title::string,
       left(updated, 10)::date as updated_dt,
       source_file_name::string,
       concat(
         cast(left(run_date, 4) as string), '-', 
         cast(substr(run_date, 5, 2) as string), '-', 
         cast(right(run_date, 2) as string)
       )::date as run_date,
       load_ts::timestamp
from source
""")

# COMMAND ----------

source_df.createOrReplaceTempView('source')

# COMMAND ----------

display(source_df.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental load to silver schema

# COMMAND ----------

if table_name not in current_silver_tables:
    spark.sql(f"""
        create table main.silver.{table_name} as
        select sha2(concat_ws('||', id, version, updated_dt), 256) as arx_sk,
               id,
               version,
               link,
               summary,
               title,
               updated_dt,
               source_file_name,
               run_date,
               load_ts
        from source
    """)
else:
    spark.sql(f"""
        with src as (
          select sha2(concat_ws('||', id, version, updated_dt), 256) as arx_sk,
                 id,
                 version,
                 link,
                 summary,
                 title,
                 updated_dt,
                 source_file_name,
                 run_date,
                 load_ts
          from source
          where updated_dt > '{watermark_date}'
        )
        merge into main.silver.{table_name} tgt
        using src
        on tgt.id = src.id
        when matched and src.version > tgt.version
        then update set *
        when not matched 
        then insert *
    """)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Validate row count post processing

# COMMAND ----------

post_rowcount = source_df.count()
post_rowcount

# COMMAND ----------

assert pre_rowcount == post_rowcount, 'Rows have been removed during transformation - this should not happen'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge metrics

# COMMAND ----------

metrics_df = spark.sql(f"""
    select operationMetrics.numTargetRowsInserted as inserted,
           operationMetrics.numTargetRowsUpdated as updated,
           operationMetrics.numOutputRows as output_rows -- For non-merge operation
    from (
    describe history main.silver.{table_name}) t 
    order by version desc
    limit 1
""")
display(metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update watermark value
# MAGIC This is the max value of the last updated date

# COMMAND ----------

max_date = str(spark.sql("select max(updated_dt) as max_date from source").collect()[0]['max_date'])
max_date

# COMMAND ----------

spark.sql(f"create or replace table main.silver.watermark_{table_name} as select '{max_date}' as watermark_date")

# COMMAND ----------

# Check updated watermark
watermark_date = spark.read.table(f'main.silver.watermark_{table_name}').collect()[0]['watermark_date']
watermark_date

# COMMAND ----------

assert watermark_date == max_date, print('Update to watermark table has failed')
