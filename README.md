# Living-recommendation

## Table of Contents
1. [Motivation](README.md#motivation)
1. [Pipline](README.md#pipline)
1. [Requirements](README.md#requirements)
1. [Architechture](README.md#architechture)
1. [DataSet](README.md#dataset)
1. [Metrics](README.md#metrics)
1. [Methodology](README.methodology)


## Motivation
People spend plenty of time to choose a new place to live. They need to check local environment, like crime rate, local hospital cost, available transportation, income level locally through different APP: 
* [nyc crime map](https://maps.nyc.gov/crime/)
* [nyc hospital map](https://www.targetmap.com/viewer.aspx?reportId=3065)
* [citybike station map](https://member.citibikenyc.com/map/)
* [google map](https://www.google.com/maps/place/New+York,+NY)
* [Median Income Across the US (WNYC)](https://project.wnyc.org/median-income-nation/#4/37.65/-85.12)

This project focus on giving proper recommendation to several factors the user selected, so that they can decide where to live easily. 


## Pipline


**Figure 1.** Pipeline depicting the flow of data.


## Requirements
* Python 3
* Ubuntu


## Architechture

### BigQuery --> Google Cloud Storage
* [Get into your GCS bucket](https://cloud.google.com/storage/?utm_source=google&utm_medium=cpc&utm_campaign=na-US-all-en-dr-bkws-all-all-trial-e-dr-1008076&utm_content=text-ad-none-any-DEV_c-CRE_79747411687-ADGP_Hybrid+%7C+AW+SEM+%7C+BKWS+%7C+US+%7C+en+%7C+EXA+~+Google+Cloud+Storage-KWID_43700007031545851-kwd-11642151515&utm_term=KW_google%20cloud%20storage-ST_google+cloud+storage&gclid=CjwKCAiA98TxBRBtEiwAVRLqu-Q98O-7xe8Fvcte79YELjXsAud44dJ95qgW3-Pgyzuixv4uZde9HhoCLKwQAvD_BwE)
* [Find dataset on BigQuery](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset)
* Click the dataset you chose, click **View Dataset**
* Write SQL query, click **RUN**.
* Click **Save Result**, choose **BigQery table**, save it under a specific database, name the table
* Find the table through left navigation bar, click **EXPORT**, choose **Export to GCS**, choose your bucket, create a name for the table

### Google cloud Storage --> S3 bucket
[Instruction](http://proanalyst.net/migrate-files-gcs-into-amazon-s3/)

### Spark
[Instruction](https://docs.google.com/document/d/1InLxbu-FH2nyd0NuJ3ewdvAt0Ttk_bNUwlQop38lq0Q/edit)
After configuration, run those command to create a new database:

    sudo -u postgres psql
    CREATE DATABASE mydb;
    CREATE USER db_select WITH PASSWORD '<setpassword>';
    GRANT ALL PRIVILEGES ON DATABASE mydb TO db_select;
    \connect mydb


### PostgreSQL
[Instruction](https://blog.insightdatascience.com/simply-install-postgresql-58c1e4ebf252)



## Dataset

* [US census data](https://console.cloud.google.com/bigquery?project=plucky-sound-238319&folder&organizationId&p=bigquery-public-data&d=census_bureau_acs&t=zip_codes_2017_5yr&page=table)
* [US census population](https://console.cloud.google.com/bigquery?project=plucky-sound-238319&folder&organizationId&p=bigquery-public-data&d=census_bureau_usa&t=population_by_zip_2010&page=table)
* [Hospital Cost](https://console.cloud.google.com/bigquery?project=plucky-sound-238319&folder&organizationId&p=bigquery-public-data&d=medicare&page=dataset)
* [New York CityBike](https://console.cloud.google.com/bigquery?project=plucky-sound-238319&folder&organizationId&p=bigquery-public-data&d=new_york_citibike&t=citibike_stations&page=table)
* [New York Subway](https://console.cloud.google.com/bigquery?project=plucky-sound-238319&folder&organizationId&p=bigquery-public-data&d=new_york_subway&t=stations&page=table)
* Crime and rolling sale dataset attached in folder Spark: **data.csv**



## Metrix


## Methodology

### Transfer Longitude and latitude to zip code:
Using <a href="https://uszipcode.readthedocs.io/index.html">uszipcode</a>, install it on pyspark.

### Calculate income level, age level and family percent level:

### Calculate crime rate:

### Calculate population density:

### Calculate real estate sale price per square feet:

