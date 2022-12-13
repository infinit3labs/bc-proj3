# Databricks notebook source
# MAGIC %md
# MAGIC ### Load Curated Data

# COMMAND ----------

df = spark.table('main.gold.vw_combined_pre_nlp')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Word Frequency
# MAGIC The purpose of the code below is to clean the article data and score it against a dictionary of relevant terms.  The output is a table of articles with relevance for the client.

# COMMAND ----------

import requests
import nltk
from pyspark.sql.functions import col, lower, regexp_replace, split, lit, size, array_distinct
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from nltk.stem import WordNetLemmatizer
from pyspark.sql.types import *

# COMMAND ----------

nltk.download('wordnet')

# COMMAND ----------

# For functions used below, inspiration from:
# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3923635548890252/1357850364289680/4930913221861820/latest.html

# COMMAND ----------

def clean_text(c):
    c = lower(c)
    c = regexp_replace(c, "^rt ", "")
    c = regexp_replace(c, "(https?\://)\S+", "")
    c = regexp_replace(c, "[^a-zA-Z0-9\\s]", "")
    return c

# COMMAND ----------

df = df.withColumn('t', clean_text(col('words')))

# COMMAND ----------

tokenizer = Tokenizer(inputCol="t", outputCol="vector")
vector_df = tokenizer.transform(df)

# COMMAND ----------

# Define a list of stop words or use default list
remover = StopWordsRemover()
stopwords = remover.getStopWords() 

# COMMAND ----------

# Specify input/output columns
remover.setInputCol("vector")
remover.setOutputCol("vector_no_stopw")

# Transform existing dataframe with the StopWordsRemover
vector_no_stopw_df = remover.transform(vector_df)

# COMMAND ----------

lemmatizer = WordNetLemmatizer()

# COMMAND ----------

def lemm(in_vec):
    out_vec = []
    for t in in_vec:
        t_lemm = lemmatizer.lemmatize(t)
        if len(t_lemm) > 2:
            out_vec.append(t_lemm)       
    return out_vec

# Create user defined function for stemming with return type Array<String>
lemmer_udf = udf(lambda x: lemm(x), ArrayType(StringType()))

# Create new df with vectors containing the stemmed tokens 
vector_lemmed_df = (
    vector_no_stopw_df
        .withColumn("vector_lemmed", lemmer_udf("vector_no_stopw"))
  )

# COMMAND ----------

def clean_tech_score(words):
    """
    Function takes an array of words and scores the whole array based on whether key terms appear.
    Weighting is totally arbitrary but is meant to score articles with more relevant terms higher.
    Output is an integer score.
    """
    _counter = 0
    # Sources:
    # https://www.cleanenergyresourceteams.org/glossary
    # https://www.tigercomm.us/cleantech-glossary-terms-and-definitions
    # https://www.cleanenergyregulator.gov.au/About/Pages/Glossary.aspx
    # https://energync.org/glossary/
    clean_tech_terms = {
        'climate': 20,
        'change': 4,
        'oxide': 1,
        'battery': 1,
        'electricity': 3,
        'abatement': 1,
        'emission': 1,
        'kyoto': 8,
        'ipcc': 20,
        'lithium': 15,
        'ion': 8,
        'photovoltaic': 25,
        'renewable': 8,
        'energy': 10,
        'solar': 8,
        'carbon': 5,
        'innovation': 20,
        'technology': 30,
        'clean': 9,
        'green': 14,
        'kilowatt': 4,
        'megawatt': 4,
        'polysilicon': 30,
        'biofuel': 40,
        'efficiency': 12,
        'fuel': 8,
        'tax': 4,
        'air': 2,
        'quality': 7,
        'bio': 8,
        'biogas': 12
    }
    for w in words:
        if w in clean_tech_terms.keys():
            _counter += clean_tech_terms[w]
    return _counter

# COMMAND ----------

score_udf = udf(lambda x: clean_tech_score(x))

# COMMAND ----------

# Create new df with vectors containing the stemmed tokens 
vector_scored_df = (
    vector_lemmed_df
        .withColumn("vector_unique", array_distinct('vector_lemmed'))
        .withColumn("vector_scored", score_udf("vector_unique"))
        .withColumn("vector_length", size('vector_unique'))
  )

# COMMAND ----------

vector_scored_df.createOrReplaceTempView('score')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table main.gold.scored_articles as
# MAGIC select *
# MAGIC from (
# MAGIC select source,
# MAGIC        source_sk,
# MAGIC        publish_dt,
# MAGIC        words,
# MAGIC        vector_scored as article_raw_score,
# MAGIC        vector_length as unique_words,
# MAGIC        1.0 * vector_scored / vector_length as article_score
# MAGIC from score) t 
# MAGIC where article_score > 0
