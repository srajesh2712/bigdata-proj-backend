# jobs/pi.py
from pyspark.sql import SparkSession
from random import random

spark = SparkSession.builder.appName("PythonPi").getOrCreate()

def inside(_):
    x, y = random(), random()
    return x*x + y*y < 1

N = 100000
count = spark.sparkContext.parallelize(range(0, N)).filter(inside).count()
print(f"Pi is roughly {4.0 * count / N}")

spark.stop()

