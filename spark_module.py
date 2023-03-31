#  Initialize Spark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# conf = SparkConf().setAppName("myApp")
# sc = SparkContext(conf=conf)
# # print(sc._jsc.sc().isStopped())
#
# #  Initialize SparkSession
# spark = SparkSession.builder.appName("myApp").getOrCreate()
def create_spark_session():
    spark = SparkSession.builder.appName("myApp").getOrCreate()
    return spark
