# Export table from Google BigQuery to Google Cloud Storage

## Table of Contents
1. [Steps to store table from Google Cloud Plantform](README.md#Steps-to-store-table-from-Google-Cloud-plantform)
1. [SQL Queries to pre-process census_bureau_acs](README.md#SQL-Queries-to-pre-process-census_bureau_acs)
1. [SQL Queries to pre-process New_York_Citybike stations](README.md#SQL-Queries-to-pre-process-New_York_Citybike-stations)
1. [SQL Queries to pre-process New_York_Subway stations](README.md#SQL-Queries-to-pre-process-New_York_Subway-stations)
1. [SQL Queries to pre-process census_bureau_population](README.md#SQL-Queries-to-pre-process-census_bureau_population)
1. [SQL Queries to pre-process ny zipcode](README.md#SQL-Queries-to-pre-process-ny-zipcode)
1. [SQL Queries to pre-process ny outpatient charge](README.md#SQL-Queries-to-pre-process-ny-outpatient-charge)
1. [SQL Queries to pre-process hospital information](README.md#SQL-Queries-to-pre-process-hospital-information)


## Steps to store table from Google Cloud Plantform
* You can find plenty of dataset in [Google Cloud Plantform](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset).
* Choose dataset, click on **VIEW DATASET**, access to BigQuery.
* Write SQL query in **Query Editor**, click on **RUN** to create the table you want (useful queries are listed below.
* Click on **SAVE RESULT**, choose **BigQuery Table**, save it.
* Find saved table from navigation bar on the left, click on **EXPORT**, select **EXPORT TO GCS**, choose your storage bucket and/or folder, creat table name, click **Select**, then **Export**. Table is successfully stored in Google Cloud Storage.

## SQL Queries to pre-process census_bureau_acs

```
select geo_id as zipcode, income_level, age_level, family_percent_level
from (
select geo_id as zipcode, income_level, age_level
from
(SELECT geo_id, 'low' as income_level
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null and median_income <= 25000
union all
SELECT geo_id, 'lower-middle' as income_level
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null and median_income > 25000 and median_income <= 50000
union all
SELECT geo_id, 'middle' as income_level
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null and median_income > 50000 and median_income <= 75000
union all
SELECT geo_id, 'higher-middle' as income_level
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null and median_income > 75000 and median_income <= 100000
union all
SELECT geo_id, 'high' as income_level
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null and median_income > 100000) t1
join 
(SELECT geo_id as id, 'young' as age_level
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null and median_age <= 35
union all
SELECT geo_id as id, 'middle' as age_level
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null and median_age > 35 and median_age <= 45
union all
SELECT geo_id as id, 'old' as age_level
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null and median_age > 45) t2
on t1.geo_id = t2.id) t3
join 
(select geo_id, 'high' as family_percent_level from (
SELECT geo_id, round(family_households*100/(nonfamily_households+family_households)) as family_percent
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null)
where family_percent >= 70
union all
select geo_id, 'median' as family_percent_level from (
SELECT geo_id, round(family_households*100/(nonfamily_households+family_households)) as family_percent
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null)
where family_percent < 70 and family_percent >= 40
union all
select geo_id, 'low' as family_percent_level from (
SELECT geo_id, round(family_households*100/(nonfamily_households+family_households)) as family_percent
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` 
where (nonfamily_households+family_households) <> 0 and total_pop <> 0 and median_income is not null)
where family_percent < 40) t4
on t3.zipcode = t4.geo_id
```


## SQL Queries to pre-process New_York_Citybike stations

```
SELECT station_id, latitude, longitude, name, region_id 
FROM `bigquery-public-data.new_york_citibike.citibike_stations`
```


## SQL Queries to pre-process New_York_Subway stations

```
SELECT station_id,station_lat,station_lon,string_agg(daytime_routes, ' ') as daytime_routes
FROM `bigquery-public-data.new_york_subway.stations` 
group by station_id,station_lat, station_lon
```


## SQL Queries to pre-process census_bureau_population

```
SELECT zipcode, sum(population) as population
FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010`
where LENGTH(zipcode) = 5
group by zipcode
order by zipcode 
```


## SQL Queries to pre-process ny zipcode
```
SELECT state_code, zip_code, area_land_meters, internal_point_lat, internal_point_lon, zip_code_geom
FROM `bigquery-public-data.geo_us_boundaries.zip_codes` 
where state_code = 'NY'
```

## SQL Queries to pre-process hospital information
1. pre-process ny outpatient charge:

```
SELECT provider_state, provider_zipcode, avg(average_total_payments) as average_outpatient_payment
FROM(
select provider_state, provider_zipcode, average_total_payments from `bigquery-public-data.medicare.outpatient_charges_2011`
union all 
select provider_state, provider_zipcode, average_total_payments from `bigquery-public-data.medicare.outpatient_charges_2012`
union all
select provider_state, provider_zipcode, average_total_payments from `bigquery-public-data.medicare.outpatient_charges_2013`
union all
select provider_state, provider_zipcode, average_total_payments from `bigquery-public-data.medicare.outpatient_charges_2014`) t1
where provider_state = 'NY'
group by provider_state, provider_zipcode
```
Save table name as: ny_outpatient_charge

2. pre-process ny inpatient charge:

```
SELECT provider_state, provider_zipcode, avg(average_total_payments) as average_inpatient_payment
FROM(
select provider_state, provider_zipcode, average_total_payments from `bigquery-public-data.medicare.inpatient_charges_2011`
union all 
select provider_state, provider_zipcode, average_total_payments from `bigquery-public-data.medicare.inpatient_charges_2012`
union all
select provider_state, provider_zipcode, average_total_payments from `bigquery-public-data.medicare.inpatient_charges_2013`
union all
select provider_state, provider_zipcode, average_total_payments from `bigquery-public-data.medicare.inpatient_charges_2014`) t1
where provider_state = 'NY'
group by provider_state, provider_zipcode
```
Save table name as: ny_inpatient_charge

3. Store all hospital information in one table:
```
select zip_code as patient_zipcode, hospital_type, count, patient_type, average_payment
from 

(select zip_code, count, hospital_type
from (
SELECT cast(zip_code as string) as zip_code, count(*) as count, 'home health agencies' as hospital_type
FROM `bigquery-public-data.cms_medicare.home_health_agencies_2014` 
group by zip_code
union all
SELECT cast(zip_code as string) as zip_code, count(*) as count, 'hospital' as hospital_type
FROM `bigquery-public-data.cms_medicare.hospital_general_info` 
group by zip_code
union all
SELECT cast(zip_code as string) as zip_code, count(*) as count, 'nursing_facility' as hospital_type
FROM `bigquery-public-data.cms_medicare.nursing_facilities_2014` 
group by zip_code)) t1

full outer join

(select cast(inpatient_zipcode as string) as patient_zipcode, average_inpatient_payment as average_payment, 'inpatient' as patient_type
from `plucky-sound-238319.cms_medical.ny_inpatient_charge`
union all
select cast(outpatient_zipcode as string) as patient_zipcode, average_outpatient_payment as average_payment, 'outpatient' as patient_type
from `plucky-sound-238319.cms_medical.ny_outpatient_charge`) t2

on t1.zip_code = t2.patient_zipcode
```
`plucky-sound-238319.cms_medical` change to your own dataset access. 

