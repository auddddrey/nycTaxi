import os

import pandas as pd
from fastapi import FastAPI
from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import SparkSession

import fare
from pyspark.sql.functions import exp

spark = SparkSession.builder.appName("myApp").getOrCreate()

#  Initialize FastAPI
app = FastAPI()

mPath = os.path.join(os.getcwd(), "models", "gbtmodel")
print(mPath)

# Load the model
model = GBTRegressionModel.load(mPath)


@app.get("/")
async def root():
    return {"message": "Health Check OK"}


@app.post('/getFare')
async def get_fare_endpoint(item: fare.Item):
    try:
        df = pd.DataFrame([item.dict().values()], columns=item.dict().keys())
        sdf = spark.createDataFrame(df, schema=fare.schema)

        # If necessary, apply any transformations to the selected columns here
        sdf = fare.transform_data(sdf, spark)
        yhat = model.transform(sdf)

        gbt_predictions = yhat.withColumn('predicted_total_amount', exp('prediction'))
        return {"total_amount_predicted": '{:.1f}'.format(gbt_predictions.select("predicted_total_amount").first()[0])}
    except Exception as e:
        return {"error": str(e)}
