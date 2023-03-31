import os

import pyspark.sql.functions as F
from pydantic import BaseModel
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Load the pipeline model
transform_model = os.path.join(os.getcwd(), "models", "pipelinetransformer");
pipeline_model = PipelineModel.load(transform_model)

# Define the schema for the input data
schema = StructType([
    StructField("pickup_zone_id", StringType(), True),
    StructField("dropoff_zone_id", StringType(), True),
    # StructField("is_weekday", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("hour", IntegerType(), True)
])


# Define the input data class Pydantic model
class Item(BaseModel):
    pickup_zone_id: str
    dropoff_zone_id: str
    #is_weekday: int
    day_of_week: int
    hour: int


# Define the input data columns
INPUT_COLS = ["pickup_zone_id", "dropoff_zone_id", "day_of_week", "hour"]


# Define the data transformation function
def transform_data(sdf, spark):
    # Prepare input data for model
    sdf = sdf.select(INPUT_COLS)

    sdf = sdf.withColumn("trip", F.concat(F.col("pickup_zone_id"), F.lit("_"), F.col("dropoff_zone_id")))
    sdf = sdf.withColumn("is_weekday", F.when((sdf.day_of_week >= 2) & (sdf.day_of_week <= 6), 1).otherwise(0))
    sdf = sdf.drop("pickup_zone_id", "dropoff_zone_id")

    # Load the input data, convert the categorical columns to numeric
    cat_string_columns = ["trip"]
    #cat_columns = ["trip_index", "is_weekday", "hour"]
    cat_columns = ["trip_index", "is_weekday", "day_of_week", "hour"]
    sdf = pipeline_model.transform(sdf)
    sdf = sdf.drop(*(cat_string_columns + cat_columns))

    feature = VectorAssembler(inputCols=sdf.columns, outputCol="features")
    feature_vector = feature.transform(sdf)
    return feature_vector
