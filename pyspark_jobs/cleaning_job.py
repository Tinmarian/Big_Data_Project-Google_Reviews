import pandas as pd
import os

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pyspark.pandas as ps


# Create a SparkSession
spk = SparkSession.builder.appName("PySpark Transformations to Populate our Data Warehouse").getOrCreate()
# cwd = os.getcwd()


states = ['California','Texas','New_York','Colorado','Georgia']


for state in states:
    i = 1
    df_list = []
    psdfx = ps.DataFrame()
    while True:
        try:
            sdf = spk.read.json(f'gs://data-lake-henry/{state}_{i}.json')
            psdf = sdf.pandas_api()
            df_list.append(psdf)
            i += 1
        except AnalysisException:
            break
    psdfx = ps.concat(df_list,axis=0)

    sdf = psdf.to_spark()

    sdf.write.mode('overwrite').format('json').save(f'gs://dataproc-pyspark-ops/out_dataproc/all_{state}_raw')