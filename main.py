from pyspark.sql import SparkSession
from random import random

#spark = SparkSession.builder.appName("PythonPi").getOrCreate()

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("My Spark Application") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()
#.config("spark.executor.memory", "2g") \
#.config("spark.executor.cores", "2") \


sc = spark.sparkContext
print(sc.uiWebUrl)
num_samples = 100_000_000

def inside(p):
    x, y = random(), random()
    return x*x + y*y < 1

count = sc.parallelize(range(num_samples)).filter(inside).count()

pi = 4 * count / num_samples
print("Pi is roughly %f" % pi)


