from uszipcode import SearchEngine
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import  monotonically_increasing_id
from pyspark.sql.types import *
from configparser import ConfigParser
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, when
from pyspark.sql import functions
from pyspark import SparkConf, SparkContext
from collections import Counter
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from configparser import ConfigParser
from os.path import abspath

# -- Load Configuration --
config = ConfigParser()
config.read(abspath('config.ini'))

# load dataset from s3
def load(file_path):
    spark = SparkSession.builder.appName('census').getOrCreate()
    bucket = config.get('AWS', 'bucket')
    path = 's3a://{}/{}'.format(bucket, file_path)
    csv = spark.read.csv(path, header=True)
    return csv

# save table to postgresql
def save(data, table, mode='append'):
    username = config.get('PostgreSQL', 'username')
    password = config.get('PostgreSQL', 'password')
    instance = config.get('PostgreSQL', 'instance')
    database = config.get('PostgreSQL', 'database')
    url = 'jdbc:postgresql://{}:5432/{}'.format(instance, database)

    data.write.format('jdbc') \
    .option("url", url) \
    .option("dbtable",table) \
    .option("user", username) \
    .option("password",password) \
    .option("driver", "org.postgresql.Driver") \
    .mode(mode).save()

# convert longitude and latitude to zipcode
def zipcode(table, sc):
    search = SearchEngine()
    csvi = table.select("*").withColumn("id", monotonically_increasing_id())
    list1 = []
    for i in range(csvi.count()):
        lon1 = csvi.where(csvi.id == i).select('longitude').collect()[0]['longitude']
        lon = float(lon1)
        lat1 = csvi.where(csvi.id == i).select('latitude').collect()[0]['latitude']
        lat = float(lat1) 
        zipcode = search.by_coordinates(lat, lon, radius=3)
        list1.append(zipcode[0].zipcode)
    row = Row("pid", "zipcode")
    new_df = sc.parallelize([row(i, list1[i]) for i in range(0,len(list1))]).toDF()
    station = csvi.join(new_df, csvi.id==new_df.pid).select("*")
    return station

# count the citybile station number
def citybike_station_count(csv_bike1):
    csv_bike = csv_bike1.groupBy('zipcode').count().select('zipcode', col("count").alias("citybike_station_num"))
    return csv_bike

# prepare subway table with different subway route:
def subway_route(csv_sub1):
    csv_sub2 = csv_sub1.withColumn('daytime_routes',explode(split('daytime_routes',' '))).select("*")
    csv_sub3 = csv_sub2.groupBy('zipcode', 'daytime_routes').count().select('zipcode', col("daytime_routes").alias("subway_routes"), col("count").alias("subway_station_num"))
    return csv_sub3

# calculate average sale price per square feet from rolling sale data after 2013:
# follow this: https://maps.nyc.gov/crime/ 
def avg_price_per_square_feet(csv_sale_crime):
    sale = csv_sale_crime.where("sale_year > 2013")
    sale = sale\
        .withColumn("price_square_feet", (col("sale_price") / col("land_square_feet")))\
        .select('zip_code', 'building_age', 'price_square_feet')
    sale = sale.select(sale.zip_code, sale.building_age, sale.price_square_feet.cast(IntegerType()))
    sale=sale.groupBy('zip_code').agg(functions.avg('price_square_feet'), functions.avg('building_age'))
    sale = sale.select('zip_code', col("avg(price_square_feet)").alias("price_square_feet").cast(IntegerType()), col("avg(building_age)").alias("building_age").cast(IntegerType()))
    return sale

# calculate the crime index by crime dataset and population dataset
def crime_index(csv_sale_crime, csv_population):
    crime = csv_sale_crime.where("sale_year = 2018").join(csv_population, csv_sale_crime.zip_code==csv_population.zipcode).select("*")
    crime_zip = crime.groupBy('zip_code', 'precinct').count()
    crime_zip = crime_zip.groupBy('precinct').count().select('count', col("precinct").alias("precinct_zip"))
    crime = crime.join(crime_zip, crime_zip.precinct_zip==crime.precinct).select('*')
    crime = crime\
        .withColumn("crime_index", ( ( (col("misdemeanor") + col("non_major_felony") + col("major_felony") + col("violation"))/ (col("population")*col("count")) )*1000 ))\
        .groupBy('zipcode', 'crime_index').count()\
        .select('zipcode', 'crime_index')
    crime = crime.withColumn("crime_index", \
        when(crime["crime_index"] >= 100, 100).otherwise(crime["crime_index"]))
    return crime

# calculate population density by population and zipcode land square feet data
def population_density(csv4):
    csv_5 = csv4\
        .withColumn("population_density", (col("population")*1000000 / col("area_land_meters")))\
        .drop('population')
    return csv_5

# preprocess 311 complain data:
def preprocess_complain(complain):
    # calculate complain count for different complain type
    complain = complain.groupBy('Incident Zip','Complaint Type').count()
    complain = complain.withColumnRenamed('Complaint Type', 'complaint')
    complain.show()
    # choose the useful complain type that users may care about
    c1 = complain.where((col('complaint') == 'Noise - Residential') | (col('complaint') == 'Noise') | (col('complaint') == 'Noise - Commercial') | (col('complaint') == 'Noise - Vehicle') | (col('complaint') == 'Noise - Street/Sidewalk')).drop('complaint')
    c1 = c1.groupBy('Incident Zip').agg(functions.sum('count'))
    c1 = c1.withColumnRenamed('sum(count)', 'Noise').withColumnRenamed('Incident Zip', 'incid_zipcode')
    c1 = c1.withColumn('quite_community', (1/col('Noise')))
    # c1.show()
    c2 = complain.where(col('complaint') == 'Street Condition').drop('complaint')
    c2 = c2.withColumnRenamed('count', 'street_condition').withColumnRenamed('Incident Zip', 'incid_zip1')
    c2 = c2.withColumn('good_street_condition', (1/col('street_condition')))
    # c2.show()
    comp = c1.join(c2, c1.incid_zipcode==c2.incid_zip1, how = 'full_outer').drop('incid_zip1')
    comp = comp.select('incid_zipcode', 'quite_community', 'good_street_condition')
    print('complain')
    comp.show()
    return comp

# convert column to another type
def convertColumn(df, names, newType):
    for name in names: 
        df = df.withColumn(name, df[name].cast(newType))
    return df 

# fill null by mean value
def fill_with_mean(ml, todos): 
    mean = ml.agg(*(functions.mean(c).alias(c) for c in todos))
    meaninfo = mean.first().asDict()
    meandf = ml.fillna(meaninfo)
    return meandf


