# NYC Taxi prediction
 group project -Group 12 UWaterloo Big Data 2023

# Use Fast Api to Deploy the Pyspark Model
# What needed for create an end point?
- Save the data transformation pipeline 
  ```
  from pyspark.ml import Pipeline
  pipeline_transformer = pipeline.fit(df)
  pipeline_transformer.write().overwrite().save("$NAMEOFYOURPIPELINE")
  ```
- Save the best model (Please indicate which model you are using e.g. linear regression, random forest, etc.)
  ```
  from pyspark.ml import Pipeline
  model.write().overwrite().save("$MODELNAME")
  ```
- Zip the above two folders and upload to nycTaxi/models or email to the group members
  
# Deploy the application on AWS EC2 instance
- Create an EC2 instance with Ubuntu
- Deploy the application on the EC2 instance
- Use NGINX as web server

