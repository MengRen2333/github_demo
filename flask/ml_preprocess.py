#!/usr/bin/env python
# -*- coding: utf-8 -*- 
# ml 
import function
from pyspark import SparkConf, SparkContext

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql import functions
sc = SparkContext()
sc.setLogLevel("ERROR")


# table
path_GDP = 'ml/GDP.txt'
GDP = function.load(path_GDP)
GDP.show()
path_income = 'ml/ny_income_vacanhouse.csv'
income = function.load(path_income)
income = income.select(col("zip_code").alias("income_zipcode"),'year','median_age','median_income','family_percent', 'vacant_housing_percent', 'percent_income_spent_on_rent')
path_sale = 'ml/prepared.csv'
sale = function.load(path_sale)
path_complain = 'ml/Retail_Food_Stores.csv'
complain = function.load(path_complain)


complain = complain.groupBy('zipcode').count()
food = complain.withColumnRenamed('zipcode', 'food_zipcode').withColumnRenamed('count', 'food_store_num')


sale = sale\
    .where("sale_price > 1000")
sale = sale\
    .withColumn("price_square_feet", (col("sale_price") / col("land_square_feet")))\
    .drop('sale_price', 'land_square_feet')
# sale = sale.select(sale.zip_code, sale.building_age, sale.price_square_feet.cast(IntegerType()))
sale=sale\
    .groupBy('zip_code','sale_year')\
    .agg(functions.avg('price_square_feet'), functions.avg('building_age'), functions.avg('commercial_units'), functions.avg('total_units'), functions.avg('gross_square_feet'), functions.avg('misdemeanor'), functions.avg('non_major_felony'), functions.avg('major_felony'), functions.avg('violation'),)
sale = sale\
    .select('zip_code','sale_year', col("avg(price_square_feet)").alias("price_square_feet").cast(IntegerType()), col("avg(building_age)").alias("building_age").cast(IntegerType()), col("avg(commercial_units)").alias("commercial_units").cast(IntegerType()), col("avg(total_units)").alias("total_units").cast(IntegerType()), col("avg(gross_square_feet)").alias("gross_square_feet").cast(IntegerType()), col("avg(misdemeanor)").alias("misdemeanor").cast(IntegerType()), col("avg(non_major_felony)").alias("non_major_felony").cast(IntegerType()), col("avg(major_felony)").alias("major_felony").cast(IntegerType()), col("avg(violation)").alias("violation").cast(IntegerType()))
# sale = function.avg_price_per_square_feet(sale)
sale.show()


sale_lable = sale\
    .withColumn("predict_year", (col("sale_year") - 1))\
    .select(col("predict_year").cast(IntegerType()), col("price_square_feet").alias("predict_price_square_feet"), col("zip_code").alias("pre_zip_code"))

sale1 = sale.join(sale_lable, (sale.sale_year==sale_lable.predict_year) & (sale.zip_code==sale_lable.pre_zip_code), how = 'left_outer').drop('predict_year','pre_zip_code')
sale1.show()

# join them together
ml = sale1.join(GDP, sale1.sale_year==GDP.GDP_year, how = 'left_outer').drop('GDP_year')
ml = ml.join(income, (ml.sale_year==income.year) & (ml.zip_code==income.income_zipcode), how = 'left_outer').drop('year','income_zipcode')
ml = ml.join(food, ml.zip_code==food.food_zipcode, how = 'left_outer').drop('food_zipcode')
ml = ml.select('sale_year','zip_code','price_square_feet','building_age','commercial_units','total_units','gross_square_feet','misdemeanor','non_major_felony','major_felony','violation','GDP_growth_rate','median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent','food_store_num','predict_price_square_feet')

# fill null by mean value
todos = ['food_store_num','median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent','misdemeanor', 'non_major_felony', 'major_felony' ,'violation']
ml_fill = function.fill_with_mean(ml, todos)

# show exist null value for every columns
ml_fill.agg(*[(1-(functions.count(c) /functions.count('*'))).alias(c+'_missing') for c in ml_fill.columns]).show()

# split them to train and predict dataset
ml_predict = ml_fill.where('sale_year = 2019').drop('predict_price_square_feet')
ml = ml_fill.where('sale_year < 2019')


# save data to local computer
ml.write.csv('/home/ubuntu/s3sp/s3sp2/ml.csv')
ml_predict.write.csv('/home/ubuntu/s3sp/s3sp2/ml_predict.csv')

