from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# define the hour zone
a = range(24)
HrZone = {x: 'Morning' for x in a[:6]}
HrZone.update({x: 'MidDay' for x in a[6:18]})
HrZone.update({x: 'Evening' for x in a[18:]})

# Define the schema for the input data
Input_schema = StructType([
    StructField("trip", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("HrZone", StringType(), True),
    StructField("counts", LongType(), True)
])

