from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col
from pyspark.sql import functions
import function
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as f
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.feature import OneHotEncoderEstimator
# Import all from `sql.types`
from pyspark.sql.types import *
# sc = SparkContext()
# sc.setLogLevel("ERROR")

class ml_preprocess:
    def __init__(self,sc):
        path_GDP = 'ml/GDP.txt'
        self.GDP = function.load(path_GDP)
        path_income = 'ml/ny_income_vacanhouse.csv'
        self.income = function.load(path_income)
        path_sale = 'ml/prepared.csv'
        self.sale = function.load(path_sale)
        path_food = 'ml/Retail_Food_Stores.csv'
        self.food = function.load(path_food)
        path_sub = 'processed/subway_station.csv'
        self.csv_sub = function.load(path_sub)
	    self.sc = sc

    def sale_preprocess(self,sale):
        sale = self.sale.where("sale_price > 1000")
        sale = sale\
            .withColumn("price_square_feet", (col("sale_price") / col("land_square_feet")))\
            .drop('sale_price', 'land_square_feet')
        sale=sale\
            .groupBy('zip_code','sale_year')\
            .agg(functions.avg('price_square_feet'), functions.avg('building_age'), functions.avg('commercial_units'), functions.avg('total_units'), functions.avg('gross_square_feet'), functions.avg('misdemeanor'), functions.avg('non_major_felony'), functions.avg('major_felony'), functions.avg('violation'),)
        sale = sale\
            .select('zip_code','sale_year', col("avg(price_square_feet)").alias("price_square_feet").cast(IntegerType()), col("avg(building_age)").alias("building_age").cast(IntegerType()), col("avg(commercial_units)").alias("commercial_units").cast(IntegerType()), col("avg(total_units)").alias("total_units").cast(IntegerType()), col("avg(gross_square_feet)").alias("gross_square_feet").cast(IntegerType()), col("avg(misdemeanor)").alias("misdemeanor").cast(IntegerType()), col("avg(non_major_felony)").alias("non_major_felony").cast(IntegerType()), col("avg(major_felony)").alias("major_felony").cast(IntegerType()), col("avg(violation)").alias("violation").cast(IntegerType()))

        return sale

    def convertColumn(self,df, names, newType):
        for name in names: 
            df = df.withColumn(name, df[name].cast(newType))
        return df 

    def station_count(self,table,zip_rename,count_rename):
        table = table.groupBy('zipcode').count().select(col('zipcode').alias(zip_rename), col('count').alias(count_rename))
        return table

    # fill null by mean value
    def fill_with_mean(self,ml, todos): 
        mean = ml.agg(*(functions.mean(c).alias(c) for c in todos))
        meaninfo = mean.first().asDict()
        meandf = ml.fillna(meaninfo)
        return meandf

    def check_null(self,ml_show):
        ml_show.agg(*[(1-(functions.count(c) /functions.count('*'))).alias(c+'_missing') for c in ml_show.columns]).show()


    def run(self):
        subway = function.zipcode(self.csv_sub, self.sc)
        subway = self.station_count(subway, 'sub_zipcode', 'sub_station_num')
        food = self.station_count(self.food, 'food_zipcode', 'food_store_num')
        income = self.income.select(col("zip_code").alias("income_zipcode"),'year','median_age','median_income','family_percent', 'vacant_housing_percent', 'percent_income_spent_on_rent')
        sale = self.sale_preprocess(self.sale)
        sale_lable = sale\
            .withColumn("predict_year", (col("sale_year") - 1))\
            .select(col("predict_year").cast(IntegerType()), col("price_square_feet").alias("predict_price_square_feet"), col("zip_code").alias("pre_zip_code"))

        sale1 = sale.join(sale_lable, (sale.sale_year==sale_lable.predict_year) & (sale.zip_code==sale_lable.pre_zip_code), how = 'left_outer').drop('predict_year','pre_zip_code')
        sale1.show()

        ml = sale1.join(self.GDP, sale1.sale_year==self.GDP.GDP_year, how = 'left_outer').drop('GDP_year')
        ml = ml.join(income, (ml.sale_year==income.year) & (ml.zip_code==income.income_zipcode), how = 'left_outer').drop('year','income_zipcode')
        ml = ml.join(food, ml.zip_code==food.food_zipcode, how = 'left_outer').drop('food_zipcode')
        ml = ml.join(subway, ml.zip_code==subway.sub_zipcode, how = 'left_outer').drop('sub_zipcode')
        ml = ml.select('sale_year','zip_code','price_square_feet','building_age','commercial_units','total_units','gross_square_feet','misdemeanor','non_major_felony','major_felony','violation','GDP_growth_rate','median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent','food_store_num','sub_station_num','predict_price_square_feet')
        ml.show()
        # ml.write.csv('/home/ubuntu/s3sp/s3sp2/ml__.csv')

        # fill null by mean value
        todos = ['food_store_num','sub_station_num','median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent','misdemeanor', 'non_major_felony', 'major_felony' ,'violation']
        ml_fill = self.fill_with_mean(ml, todos)

        # show exist null value for every columns
        ml_show = ml_fill
        # ml_show.agg(*[(1-(functions.count(c) /functions.count('*'))).alias(c+'_missing') for c in ml_fill.columns]).show()
        self.check_null(ml_show)
        # drop null value from column
        

        # split them to train and predict dataset
        ml_predict = ml_fill.where('sale_year = 2019').drop('predict_price_square_feet')
        
        ml = ml_fill.where('sale_year < 2019')
        ml = ml.where(col("predict_price_square_feet").isNotNull())

        ml.show()
        ml_predict.show()
        return ml, ml_predict, sale






# process = ml_preprocess(sc)
# ml, ml_predict,sale = process.run()
# ml.show()
# ml_predict.show()
# sale.show()