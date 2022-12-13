# Data Engineering Camp - Project 3

This repo is a collection of artifacts used to develop an end-to-end cloud data pipeline.

The use case is to collect article data from various API sources to identify trends in clean tech.

The process is as follows:
* extract and load data from APIs into S3 buckets using Airbyte
* mount S3 to an Azure Databricks workspace
* run a Databricks workflow to clean and curate data through Bronze > Silver > Gold layers
* TODO: apply NLP techniques to extract information about the relevance of each article for the purpose of monitoring clean tech trends

All process are orchestrated through Airflow that is configured to run daily.

For the pipeline to work, the following set up is required:
* creation of an S3 bucket with an IAM account to read and write to the bucket
* creation of a VM in Azure that has Airflow and Airbyte installed (via Docker Compose) - NOTE: When connecting from Airflow to Airbyte on the same instance, I needed to whitelist the public IP address of the VM on port 8001 so that Airflow could connect (even though it is the same machine).  Localhost and connecting the two Docker applications together failed in my testing.
* creation of an Airbyte connection to extract data from the New York Times API and load into S3 (there are multiple endpoints available in the in-built Airbyte connector, all of which are loaded, but only the Archive data is used)
* creation of a custom Airbyte connector to extract data from the Serp API (which has a service which scrapes Google Scholar data) - an account allows for 100 queries per month for non-commercial use
* creation of an Airbyte connection to extract data from the Serp API and load into S3
* creation of custom Python code to modify the NY Times source connector to read data from the correct month (which is derived from the run date)
* creation of custom Python code to extract data from the Arxiv API and load into S3
* creation of relevant Airflow connections (Airbyte and Databricks) as well as variables (for connection ids)
* creation of a Databricks workspace and cluster (I set up with an ML runtime and enabled Unity catalog)
* creation of a Databricks Connector service principal to allow Unity Catalog to communicate with ADLS
* creation of an ADLS account and a container to host a metastore for Databricks.  I opted to use 'managed' Delta tables which were stored in a separate metastore that was linked to my workspace.  Ideally, deleting the workspace will not impact the use of the metastore (achieving the same outcome of using unmanaged tables I believe)
* creation of bronze, silver, and gold layer notebooks in Databricks to process the data
* implementation of parameters and quality checks in the notebooks to allow for idempotent operation
* creation of a workflow to link all notebooks together in a pipeline