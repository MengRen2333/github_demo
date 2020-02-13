import function
from pyspark import SparkContext 
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.feature import OneHotEncoderEstimator
# Import all from `sql.types`
from pyspark.sql.types import *

sc = SparkContext()
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

# load data locally
data_all = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('median_replaced_train.csv')
# choose different features
# data = data_all.drop('sale_year','median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent')
# data = data_all.drop('sale_year','commercial_units','major_felony','GDP_growth_rate')
data = data_all.drop('sale_year')
# data = data_all

data = data.na.drop()



# Write a custom function to convert the data type of DataFrame columns

# List of continuous features
# for different feature
# CONTI_FEATURES  = ['predict_price_square_feet','price_square_feet','building_age','gross_square_feet','misdemeanor', 'non_major_felony', 'major_felony' ,'violation','GDP_growth_rate']
CONTI_FEATURES  = ['median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent', 'predict_price_square_feet','price_square_feet','building_age','gross_square_feet','misdemeanor', 'non_major_felony', 'major_felony' ,'violation','GDP_growth_rate']
# CONTI_FEATURES  = ['median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent', 'predict_price_square_feet','price_square_feet','building_age','gross_square_feet','misdemeanor', 'non_major_felony', 'violation']

# Convert the type
data = function.convertColumn(data, CONTI_FEATURES, FloatType())
# Check the dataset
data.printSchema()



data = data.withColumnRenamed('predict_price_square_feet', 'label')
data.show()

feature_list = []
for col in data.columns:
    if col == 'label':
        continue
    else: 
        feature_list.append(col)

assembler = VectorAssembler(inputCols=feature_list, outputCol="features")



# Split the data into train and test sets
train_data, test_data = data.randomSplit([.8,.2],seed=1234)


from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize `rf`
rf = RandomForestRegressor(labelCol="label", featuresCol="features")
pipeline = Pipeline(stages=[assembler, rf])


# find the best parameters
from pyspark.ml.tuning import ParamGridBuilder
import numpy as np

paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [int(x) for x in np.linspace(start = 10, stop = 50, num = 3)]) \
    .addGrid(rf.maxDepth, [int(x) for x in np.linspace(start = 5, stop = 25, num = 3)]) \
    .build()

# cross validate the result
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=RegressionEvaluator(),
                          numFolds=3)

cvModel = crossval.fit(train_data)
predictions = cvModel.transform(test_data)
predictions.select("prediction", "label", "features").show(5)


#EVALUATE
#To measure the success of this model, the RegressionEvaluator function calculates the RMSE of the model predictions. 
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Model Performance RMSE: %f" % rmse)
eval_r2 = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="r2")
print('R2:', eval_r2.evaluate(predictions))

# feature importance 
bestPipeline = cvModel.bestModel
bestModel = bestPipeline.stages[1]
importances = bestModel.featureImportances
x_values = list(range(len(importances)))

# best hyper parameters
print('numTrees - ', bestModel.getNumTrees)
print('maxDepth - ', bestModel.getOrDefault('maxDepth'))



# pre process the test dataset
test = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('median_replaced_test.csv')
# try different features
# data = data_all.drop('sale_year','median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent')
# data = data_all.drop('sale_year','commercial_units','major_felony','GDP_growth_rate')
test = test.drop('sale_year')
CONTI_FEATURES  = ['median_age','median_income','family_percent','vacant_housing_percent','percent_income_spent_on_rent','price_square_feet','building_age','gross_square_feet','misdemeanor', 'non_major_felony', 'major_felony' ,'violation','GDP_growth_rate']
test = function.convertColumn(test, CONTI_FEATURES, FloatType())
print('test')
test.show()
prediction_final = cvModel.transform(test)


# prediction_final.select("prediction", "features").show(5)
prediction_final = prediction_final.withColumnRenamed('zip_code', 'pre_zip_code')
prediction_final = prediction_final.withColumnRenamed('price_square_feet', 'pre_price_square_feet')
prediction_f = prediction_final.select('pre_zip_code','pre_price_square_feet','prediction')
prediction_f2 = prediction_f.na.drop()
string_features = ['pre_zip_code']
prediction_f2 = function.convertColumn(prediction_f2, string_features, StringType())

print('prediction_final')

# calculate price growth rate based on prediction price
prediction_f2 = prediction_f2.withColumn("price_growth_rate", ((f.col('prediction')-f.col('pre_price_square_feet'))*100/f.col('pre_price_square_feet')))
prediction_f2.printSchema()
prediction_f2.show()


# join prediction with previous data 
path_sale = 's3a://enjoyablecat/ml/prepared.csv'
sale = function.load(path_sale)

sale = sale\
    .where("sale_price > 1000")
sale = sale\
    .withColumn("price_square_feet", (f.col("sale_price") / f.col("land_square_feet")))\
    .drop('sale_price', 'land_square_feet')
# sale = sale.select(sale.zip_code, sale.building_age, sale.price_square_feet.cast(IntegerType()))
sale=sale\
    .groupBy('zip_code','sale_year')\
    .agg(f.avg('price_square_feet'))
sale = sale\
    .select('zip_code','sale_year', f.col("avg(price_square_feet)").alias("price_square_feet").cast(IntegerType())).withColumn('price_growth_rate', f.lit(null).cast(StringType))
# sale = function.avg_price_per_square_feet(sale)
sale.show()

# set prediction as year 2020 price square feet
prediction_f = prediction_f2.withColumn('Country',f.lit(2020)).select('pre_zip_code', 'prediction','Country','price_growth_rate')
prediction_f = prediction_f.withColumnRenamed('Country', 'sale_year').withColumnRenamed('prediction', 'price_square_feet').withColumnRenamed('pre_zip_code', 'zip_code')
sale2 = sale.select('zip_code','sale_year','price_square_feet','price_growth_rate')
prediction_f = prediction_f.unionAll(sale2)
prediction_f.show()

# store the table
table = 'price_all_predict'
function.save(prediction_f, table)



