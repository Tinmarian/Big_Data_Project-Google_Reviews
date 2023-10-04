import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.pandas as ps





# Create a SparkSession
spk = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()

columns = ['first','second','third']
var = ['Hello_PySpark', 'this is my first DIY job on pyspark', 'let\'s do this']

prueba = spk.createDataFrame(var, StringType())

prueba.write.mode('overwrite').format('text').save('gs://data-lake-henry/out_dataproc/prueba1.txt')

df = spk.read.text('gs://data-lake-henry/pyspark-jobs/testing_job.txt')

df.write.mode('overwrite').format('text').save('gs://data-lake-henry/out_dataproc/prueba2.txt')

# gs://data-lake-henry/pyspark-jobs/testing_job.txt

#
# lista = spk.textFile('gs://data-lake-henry/pyspark-jobs/testing_job.txt')
# df = spk.createDataFrame
#
#
#
#
#