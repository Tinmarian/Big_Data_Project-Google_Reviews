import math
import pandas as pd
import numpy as np
import os

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import IntegerType,StructField,StructType,StringType,LongType,ArrayType
import pyspark.pandas as ps

# Create a SparkSession
spk = SparkSession.builder.appName("PySpark Transformations to Populate our Data Warehouse").getOrCreate()
# cwd = os.getcwd()

states = ['California','Texas','New_York','Colorado','Georgia']
schema = StructType([
    StructField('user_id',LongType(),False),
    StructField('name',StringType(),True),
    StructField('time',LongType(),True),
    StructField('rating',IntegerType(),True),
    StructField('text',StringType(),True),
    StructField('resp',ArrayType(),True),
    StructField('gmap_id',StringType(),False)
])

for state in states:
    
    # Concatenamos los diferentes archivos que tenemos para cada estado.
    i = 1
    df_list = []
    psdfx = ps.DataFrame()
    while True:
        try:
            # Leemos los archivos en un SPARK Data Frame para poder acceder directamente a GCS
            sdf = spk.read.schema(schema).json(f'gs://data-lake-henry/{state}_{i}.json')[['user_id','name','time','rating','text','resp','gmap_id']]
            # PANDAS API Data Frame: Paso intermedio para generar un PANDAS Data Frame.
            psdf = sdf.pandas_api()
            df_list.append(psdf)
            i += 1
        except AnalysisException:
            break
    psdfx = ps.concat(df_list,axis=0)
    
    # Generamos el primer grupo de transformaciones para los datos de las reviews de Maps en PANDAS API. Queda la metadata y los archivos de Yelp.
    psdfx['resp_time'] = ps.Series(dtype=np.int64)
    psdfx['resp_text'] = ps.Series(dtype=np.int64)
    for i in range(len(psdfx)):
        if type(psdfx.resp[i]) == dict:
            psdfx.loc[i,'resp_time'] = psdfx.resp[i]['time']
            psdfx.loc[i,'resp_text'] = psdfx.resp[i]['text']
        else:
            psdfx.loc[i,'resp_time'] = 1
            psdfx.loc[i,'resp_text'] = ''
    psdfx.resp_time = psdfx.resp_time.fillna(0).astype('int64')
    psdfx.resp_text = psdfx.resp_text.fillna('')
    psdfx = psdfx[['user_id', 'name', 'time', 'rating', 'text', 'gmap_id', 'resp_time', 'resp_text']]

    # Convertimos el dataframe de Pandas API on Spark a un dataframe de Spark
    sdf = psdf.to_spark()

    # Guardamos las tablas concatenadas en archivos .json en GCS.
    sdf.write.mode('overwrite').format('csv').save(f'gs://dataproc-pyspark-ops/out_dataproc/staging/all_{state}_raw')
