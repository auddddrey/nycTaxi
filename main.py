import os
from typing import Union

import mlflow
import pandas as pd
from fastapi import FastAPI
from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import exp

import fare
import traffic

spark = SparkSession.builder.appName("myApp").getOrCreate()

#  Initialize FastAPI
app = FastAPI()

mPath = os.path.join(os.getcwd(), "models", "gbtmodel")
print(mPath)

# Load the model
model_fare = GBTRegressionModel.load(mPath)

# load traffic model
model_uri = os.path.join(os.getcwd(), "models", "traffic-model")
model_traffic = mlflow.spark.load_model(model_uri=model_uri)


@app.get("/")
async def root():
    return {"message": "Health Check OK"}


# Get fare
@app.post('/getFare')
async def get_fare_endpoint(item: fare.Item):
    try:
        df = pd.DataFrame([item.dict().values()], columns=item.dict().keys())
        sdf = spark.createDataFrame(df, schema=fare.schema)

        # If necessary, apply any transformations to the selected columns here
        sdf = fare.transform_data(sdf, spark)
        yhat = model_fare.transform(sdf)

        gbt_predictions = yhat.withColumn('predicted_total_amount', exp('prediction'))
        return {"total_amount_predicted": '{:.1f}'.format(gbt_predictions.select("predicted_total_amount").first()[0])}
    except Exception as e:
        return {"error": str(e)}


#   Get traffic volumn
#  @param hour: 0-23, src_dst: 237,236
@app.get("/traffic/{hour}")
async def read_traffic_volume(hour: int, src_dst: Union[str, None] = None):
    try:
        counts = '-1'

        if src_dst is None:
            return {"error": "src_dst parameter is missing"}

        trip = '{}_{}'.format(*src_dst.split(','))
        ltrip = ["237_236", "264_264", "236_237", "237_237", "236_236", "237_161", "161_237",
                 "161_236", "239_142", "142_239", "239_238", "141_236", "236_161"]
        if not trip in ltrip:
            return {"hour": hour, "counts": counts}

        df_prepped = spark.createDataFrame(
            pd.DataFrame(data={'trip': [trip], 'hour': [hour], 'HrZone': [traffic.HrZone[hour]], 'counts': [0]}),
            schema=traffic.Input_schema)

        target = 'counts'
        holdout = model_traffic.transform(df_prepped).select([target, 'prediction']).toPandas()
        counts = round(holdout.loc[0, 'prediction'])

        return {"hour": hour, "counts": str(counts)}
    except Exception as e:
        return {"error": str(e)}
