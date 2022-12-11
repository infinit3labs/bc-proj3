# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleanse bronze NY Times Archive data into silver table
# MAGIC This notebook takes the latest batch of NY Times archive data, assigns appropriate data types, and incrementally loads articles published after the prior run into a silver delta table.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text is_fresh_load default '0'

# COMMAND ----------

table_name = 'nytarchive'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code to revert to clean state if fresh load

# COMMAND ----------

if dbutils.widgets.get('is_fresh_load') == '1':
    spark.sql(f"drop table if exists main.silver.{table_name}")

# COMMAND ----------

# Does silver table exist?
current_silver_tables = [t.name for t in spark.catalog.listTables('main.silver')]
current_silver_tables

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

source_df = spark.sql("""
select _id::string as id,
       abstract::string,
       lead_paragraph::string,
       snippet::string,
       left(pub_date, 10)::date as publish_dt,
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

# MAGIC %md
# MAGIC ### Incremental load to silver schema

# COMMAND ----------

if table_name not in current_silver_tables:
    spark.sql(f"""
        create table main.silver.{table_name} as
        select sha2(concat_ws('||', id, publish_dt), 256) as nyt_sk,
               id,
               abstract,
               lead_paragraph,
               snippet,
               publish_dt,
               source_file_name,
               run_date,
               load_ts
        from source
    """)
else:
    spark.sql(f"""
        insert into main.silver.{table_name}
        select *
        from (
        select sha2(concat_ws('||', id, publish_dt), 256) as nyt_sk,
               id,
               abstract,
               lead_paragraph,
               snippet,
               publish_dt,
               source_file_name,
               run_date,
               load_ts
        from source ) t
        where nyt_sk not in (
            select nyt_sk
            from main.silver.{table_name}
        )
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate row count post processing

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

display(source_df.take(5))
