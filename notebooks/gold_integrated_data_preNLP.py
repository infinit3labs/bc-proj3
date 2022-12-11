# Databricks notebook source
# MAGIC %md
# MAGIC ## Curate silver data into gold summary table of article words

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text is_fresh_load default '0'

# COMMAND ----------

nyt_table_name = 'nytarchive'
google_scholar_table_name = 'googlescholar'
arxiv_table_name = 'arxiv'
output_view_name = 'combined_pre_nlp'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code to revert to clean state if fresh load

# COMMAND ----------

if dbutils.widgets.get('is_fresh_load') == '1':
    spark.sql(f"drop table if exists main.gold.{nyt_table_name}_words")
    spark.sql(f"drop table if exists main.gold.{google_scholar_table_name}_words")
    spark.sql(f"drop table if exists main.gold.{arxiv_table_name}_words")

# COMMAND ----------

nyt_silver_df = spark.table(f'main.silver.{nyt_table_name}')
google_silver_df = spark.table(f'main.silver.{google_scholar_table_name}')
arxiv_silver_df = spark.table(f'main.silver.{arxiv_table_name}')

# COMMAND ----------

nyt_silver_df.createOrReplaceTempView('nyt_slv')
google_silver_df.createOrReplaceTempView('ggl_slv')
arxiv_silver_df.createOrReplaceTempView('arx_slv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare NYT silver data

# COMMAND ----------

nyt_words_df = spark.sql("""
    select 'nyt' as source,
           nyt_sk,
           lower(concat_ws(' ', abstract, lead_paragraph, snippet)) as words,
           publish_dt
    from nyt_slv
""")

# COMMAND ----------

nyt_words_df.createOrReplaceTempView('nyt')

# COMMAND ----------

spark.sql(f"""
    create or replace table main.gold.{nyt_table_name}_words as
    select *
    from nyt
""")

# COMMAND ----------

nyt_count = nyt_words_df.count()
nyt_count

# COMMAND ----------

display(nyt_words_df.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Google Scholar silver data

# COMMAND ----------

ggl_words_df = spark.sql("""
    select 'ggl' as source,
           ggl_sk,
           lower(concat_ws(' ', snippet, title)) as words,
           publish_dt
    from ggl_slv
""")

# COMMAND ----------

nyt_words_df.createOrReplaceTempView('nyt')

# COMMAND ----------

spark.sql(f"""
    create or replace table main.gold.{google_scholar_table_name}_words as
    select *
    from ggl
""")

# COMMAND ----------

ggl_count = ggl_words_df.count()
ggl_count

# COMMAND ----------

display(ggl_words_df.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Arxiv silver data

# COMMAND ----------

arx_words_df = spark.sql("""
    select 'arx' as source,
           arx_sk,
           lower(concat_ws(' ', summary, title)) as words,
           updated_dt
    from arx_slv
""")

# COMMAND ----------

nyt_words_df.createOrReplaceTempView('nyt')

# COMMAND ----------

spark.sql(f"""
    create or replace table main.gold.{arxiv_table_name}_words as
    select *
    from arx
""")

# COMMAND ----------

arx_count = arx_words_df.count()
arx_count

# COMMAND ----------

display(arx_words_df.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create view of combined data

# COMMAND ----------

spark.sql(f"""
    create or replace view main.gold.vw_{output_view_name} as
    select *
    from main.gold.{nyt_table_name}_words
    union all
    select *
    from main.gold.{google_scholar_table_name}_words
    union all
    select *
    from main.gold.{arxiv_table_name}_words
""")
