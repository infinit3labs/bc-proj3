# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleanse bronze Google Scholar data into silver table
# MAGIC This notebook takes the latest batch of Google Scholar article data, assigns appropriate data types, and incrementally loads articles published after the prior run into a silver delta table.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text is_fresh_load default '0'

# COMMAND ----------

table_name = 'googlescholar'

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

source_df = spark.sql("""
select link::string,
       result_id::string,
       snippet::string,
       title::string,
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
# MAGIC ### Derive Publish Date

# COMMAND ----------

def get_days_ago(words):
    sentence_head = words[:15]
    nums = None
    if 'day ago' or 'days ago' in sentence_head:
        nums = ''.join([s for s in sentence_head.split() if s.isdigit()])
        return int(nums)

# COMMAND ----------

from pyspark.sql.types import IntegerType
spark.udf.register("days_ago", get_days_ago, returnType=IntegerType())

# COMMAND ----------

# TODO: Remove 'days ago' from head of snippet to remove false positive of word frequency

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental load to silver schema
# MAGIC Code also derives a publish date based on the snippet text and the run date.  If no 'days ago' in the snippet text, the publish date will revert to the run date - which won't be totally accurate but is the best date we have to go on.

# COMMAND ----------

if table_name not in current_silver_tables:
    spark.sql(f"""
        create table main.silver.{table_name} as
        select sha2(concat_ws('||', result_id, publish_dt), 256) as ggl_sk,
               *
        from (
        select link,
               result_id,
               snippet,
               title,
               coalesce(date_add(run_date, -days_ago(snippet)), run_date) as publish_dt,
               source_file_name,
               run_date,
               load_ts
        from source) t
    """)
else:
    spark.sql(f"""
        insert into main.silver.{table_name}
        select sha2(concat_ws('||', result_id, publish_dt), 256) as ggl_sk,
               *
        from (
        select link,
               result_id,
               snippet,
               title,
               coalesce(date_add(run_date, -days_ago(snippet)), run_date) as publish_dt,
               source_file_name,
               run_date,
               load_ts
        from main.silver.{table_name} ) t
        where publish_dt > '{watermark_date}'
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

# MAGIC %md
# MAGIC ### Review loaded data

# COMMAND ----------

check_df = spark.sql(f"select * from main.silver.{table_name} order by run_date desc limit 10")
display(check_df.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update watermark value
# MAGIC This is the run date (no article date available)

# COMMAND ----------

max_date = str(spark.sql(f"select max(publish_dt) as max_date from main.silver.{table_name}").collect()[0]['max_date'])
max_date

# COMMAND ----------

spark.sql(f"create or replace table main.silver.watermark_{table_name} as select '{max_date}' as watermark_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate watermark value updated successfully

# COMMAND ----------

# Check updated watermark
watermark_date = spark.read.table(f'main.silver.watermark_{table_name}').collect()[0]['watermark_date']
watermark_date

# COMMAND ----------

assert watermark_date == max_date, print('Update to watermark table has failed')
