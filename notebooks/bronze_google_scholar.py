# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT run_date DEFAULT ''

# COMMAND ----------

run_date = dbutils.widgets.get('run_date')
run_date

# COMMAND ----------

path = f"/mnt/bronze/google_scholar"
path

# COMMAND ----------

def get_run_date_files(run_date, path):
    assert len(run_date) == 8
    assert len(path) > 0
    _run_date_fmt = run_date[:4] + '_' + run_date[4:6] + '_' + run_date[-2:]
    _files = []
    for f in dbutils.fs.ls(path):
        if f.name[:10] == _run_date_fmt:
            _files.append(f.path)
    return _files

# COMMAND ----------

def get_latest_file(file_list):
    assert len(file_list) > 0
    _files = {}
    for f in file_list:
        _files[f.split('/')[-1].split('_')[-2]] = f
    out = _files[max(_files.keys())]
    return out

# COMMAND ----------

run_date_files = get_run_date_files(run_date, path)
if len(run_date_files) > 0:
    latest_file = get_latest_file(run_date_files)
else:
    # No run was completed on run date
    latest_file = None
    dbutils.notebook.exit(f'No files returned for run date: {run_date}')
latest_file

# COMMAND ----------

file_name = latest_file.split('/')[-1].split('.jsonl')[0]
file_name

# COMMAND ----------

if latest_file:
    df = spark.read.json(latest_file, multiLine=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.select('_airbyte_data.*')
df1 = df1.withColumn('source_file_name', F.lit(file_name))

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.createOrReplaceTempView('df')

# COMMAND ----------

display(df1)

# COMMAND ----------

out_df = spark.sql(f"""
    with data as (
    select explode(organic_results) results,
           source_file_name
    from df)
    select results.*,
           source_file_name,
           {run_date} as run_date,
           current_timestamp() as load_ts
    from data
""")

# COMMAND ----------

out_df.createOrReplaceTempView('out_df')

# COMMAND ----------

display(out_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table main.bronze.googlescholar
# MAGIC select *
# MAGIC from out_df
