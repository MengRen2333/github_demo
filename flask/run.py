import function
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as f
sc = SparkContext()
sc.setLogLevel("ERROR")


# for table ny_subway_station:
path_sub = 's3a://enjoyablecat/processed/subway_station.csv'
csv_sub = function.load(path_sub)
# transfer longitude, latitude to zip code
csv_sub1 = function.zipcode(csv_sub, sc)
csv_sub3 = function.subway_route(csv_sub1)
# save the table into postgresql
table_subway = 'subway'
# function.save(csv_sub3, url, username, password, table_subway)
function.save(csv_sub3, table_subway)

# process table ny_citibike_station:
path_bike = 's3a://enjoyablecat/processed/citybike_station'
csv = function.load(path_bike)
csv_bike1 = function.zipcode(csv,sc)
csv_bike  = function.citybike_station_count(csv_bike1)

# loading rolling sale and crime data:
path_sale_crime = 's3a://enjoyablecat/processed/prepared.csv'
csv_sale_crime = function.load(path_sale_crime)

# calculate average sale price per square feet from rolling sale data after 2013
sale = function.avg_price_per_square_feet(csv_sale_crime)

# load population dataset to cauculate crime index
path_population = 's3a://enjoyablecat/processed/census_population'
csv_population = function.load(path_population)
crime = function.crime_index(csv_sale_crime, csv_population)
crime.show()

# load retail food store data
path_food = 's3a://enjoyablecat/ml/Retail_Food_Stores.csv'
food = function.load(path_food)
food = food.groupBy('zipcode').count()
food = food.withColumnRenamed('zipcode', 'food_zipcode').withColumnRenamed('count', 'food_store_num')
food.show()

# load nyc 311 complain data and preprocess it
path_complain = 's3a://enjoyablecat/ml'
complain = function.load(path_complain)
comp = function.preprocess_complain(complain)


# for censue, hospital cost, rolling sale, zipcode table:
path_1 = 's3a://enjoyablecat/challenge/census_population.csv'
census_population = function.load(path_1)
path_2 = 's3a://enjoyablecat/challenge/census_zipcode_2017.csv'
census_zipcode_2017 = function.load(path_2)
# path_3 = 's3a://enjoyablecat/challenge/ny_inpatient_charge.csv'
# ny_inpatient_charge = function.load(path_3)
# path_4 = 's3a://enjoyablecat/challenge/ny_outpatient_charge.csv'
# ny_outpatient_charge = function.load(path_4)
path_5 = 's3a://enjoyablecat/challenge/ny_zipcode.csv'
ny_zipcode = function.load(path_5)


# join them together
csv1 = census_population.join(census_zipcode_2017, census_population.pop_zipcode == census_zipcode_2017.zipcode)
csv2 = csv1.join(ny_zipcode, csv1.zipcode == ny_zipcode.zip_code, how='right_outer')
csv4 = csv2\
    .select('zip_code', 'population', 'income_level', 'age_level', 'family_percent_level', 'area_land_meters','state_code','city','county','state_name','internal_point_lat','internal_point_lon', 'zip_code_geom')
csv4 = csv4.withColumnRenamed('zip_code', 'full_zipcode')
# calculate population density
csv_all = function.population_density(csv4)



# join all table together except subway:
csv_1 = csv_all.join(csv_bike, csv_all.full_zipcode==csv_bike.zipcode, how='left_outer').drop("zipcode")
print('csv_1')
csv_1.show()
csv_2 = csv_1.join(comp, csv_1.full_zipcode==comp.incid_zipcode, how='left_outer').drop("incid_zipcode")
print('csv_2')
csv_2.show()
csv_3 = csv_2.join(sale, csv_2.full_zipcode==sale.zip_code, how='left_outer').drop("zip_code")
print('csv_3')
csv_3.show()
csv_4 = csv_3.join(crime, csv_3.full_zipcode==crime.zipcode, how='left_outer').drop("zipcode")
print('csv_4')
csv_4.show()
csv_5 = csv_4.join(food, csv_4.full_zipcode==food.food_zipcode, how='left_outer').drop("food_zipcode")
print('csv_5')
csv_5.show()
csv_5 = csv_5.select('full_zipcode','income_level','age_level','family_percent_level','state_code','city','county','state_name','area_land_meters','internal_point_lat','internal_point_lon','population_density','citybike_station_num','price_square_feet','crime_index','food_store_num','quite_community', 'good_street_condition'
)
csv_5 = csv_5.withColumn('Country', f.lit('United States'))
csv_5.show()

# load table to postgresql:
table_full = 'full_dataset2'
# function.save(csv_5, url, username, password, table_full)
function.save(csv_5, table_full)


# store hospital data seperately
path_6 = 's3a://enjoyablecat/processed/medical_all.csv'
medical = function.load(path_6)
hospital = ny_zipcode.join(medical, ny_zipcode.zip_code == medical.patient_zipcode, how='left_outer')
hospital = hospital.select(f.col('zip_code').alias('patient_zipcode'), 'hospital_type',f.col('count').alias('hospital_count'),'patient_type','average_payment')
hospital.show()
table_hospital = 'hospital'
# function.save(hospital, url, username, password, table_hospital)
function.save(hospital, table_hospital)

