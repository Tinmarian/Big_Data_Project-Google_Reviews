import pandas as pd
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException
import pyspark.pandas as ps


cwd = os.getcwd()

# Create a SparkSession
spk = SparkSession.builder.appName("PySpark Transformations to Populate our Data Warehouse").getOrCreate()


i = 1
df_list = []
psdfx = ps.DataFrame()
while True:
    try:
        sdf = spk.read.json(f'gs://data-lake-henry/California_{i}.json')
        psdf = sdf.pandas_api()
        df_list.append(psdf)
        i += 1
    except AnalysisException:
        break
psdfx = ps.concat(df_list,axis=0)

sdf = psdf.to_spark()

sdf.write.mode('overwrite').format('json').save('gs://data-lake-henry/out_dataproc/all_california_raw')