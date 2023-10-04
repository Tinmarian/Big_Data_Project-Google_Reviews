import pandas as pd

from pyspark.sql import SparkSession

# Create a SparkSession
spk = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()

var = ['Hello_PySpark', 'this is my first DIY job on pyspark', 'let\'s do this']

prueba = spk.createDataFrame(var)

prueba.write.mode('overwrite').format('text').save('gs://data-lake-henry/out_dataproc/prueba.txt')