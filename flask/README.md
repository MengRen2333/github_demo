# Process tables

1. config.ini: tap your own database and S3 bucket information in it.
2. function.py: functions that been used.
3. run.py: preprocess all tables, join them, then add to postgreSQL. `tap folder and tables been used on path` 
4. ml_preprocess_function.py: create a class to preprocess data for machine learning model.
5. ml_model_rf: build a random forest regression model to predict future housing price.
