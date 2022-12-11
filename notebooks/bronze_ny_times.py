# Databricks notebook source
spark.conf.set("spark.sql.caseSensitive", True)
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT run_date DEFAULT ''

# COMMAND ----------

run_date = dbutils.widgets.get('run_date')
run_date

# COMMAND ----------

path = f"/mnt/bronze/archive"
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
    df = spark.read.json(latest_file)

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.select('_airbyte_data.*')
df1 = df1.withColumn('source_file_name', F.lit(file_name))

# COMMAND ----------

# Need to remove multimedia object before loading to delta as there is a duplicate key
cols = [c for c in df1.columns if c not in 'multimedia']
cols

# COMMAND ----------

df1 = df1.select(cols)

# COMMAND ----------

df1.createOrReplaceTempView('df')

# COMMAND ----------

display(df1)

# COMMAND ----------

out_df = spark.sql(f"""
    select *,
           {run_date} as run_date,
           current_timestamp() as load_ts
    from df
""")

# COMMAND ----------

out_df.createOrReplaceTempView('out_df')

# COMMAND ----------

display(out_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table main.bronze.nytarchive
# MAGIC select *
# MAGIC from out_df

# COMMAND ----------


