# Databricks notebook source
import urllib

# COMMAND ----------

# MAGIC %md
# MAGIC ### AWS S3 Secrets

# COMMAND ----------

# Set AWS Secrets - for S3 bucket
ACCESS_KEY = dbutils.secrets.get(scope = "key-vault", key = "aws-s3-access-key")
SECRET_KEY = dbutils.secrets.get(scope = "key-vault", key = "aws-s3-secret-key")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe='')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bucket Name

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = 'bc-proj3'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

# Set storage layer
LAYER = 'bronze'
# Mount name for the bucket
MOUNT_NAME = f'/mnt/{LAYER}'
# Source url
SOURCE_URL = f's3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_BUCKET}/{LAYER}'
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %fs mounts
