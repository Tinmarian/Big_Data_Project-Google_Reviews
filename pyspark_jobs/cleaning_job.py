import os
import pandas as pd
import numpy as np
import logging
# import gcsfs


from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import IntegerType,StructField,StructType,StringType,LongType,MapType
import pyspark.pandas as ps

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../credentials/fiery-protocol-399500-f2566dd92ef4.json'
PROJECT_ID = 'fiery-protocol-399500'
STATES = ['California','Texas','New_York','Colorado','Georgia']
schema = StructType([
    StructField('user_id',LongType(),False),
    StructField('name',StringType(),True),
    StructField('time',LongType(),True),
    StructField('rating',IntegerType(),True),
    StructField('text',StringType(),True),
    StructField('resp',MapType(StringType(),StringType(),True),True),
    StructField('gmap_id',StringType(),False)
])
# Create a SparkSession
spk = SparkSession.builder.appName("PySpark Transformations to Populate our Data Warehouse").getOrCreate()




# # Cargamos los metadatos de Google Maps.
# maps_metadata = ps.DataFrame()
# lista_metadata = []
# i = 1
# while True:
#     try:
#         # sdf = spk.read.format('org.apache.spark.sql.json').json(f'gs://data-lake-henry/metadata_{i}.json',multiline=True)[['gmap_id','name','address','avg_rating','num_of_reviews','price']]
#         psdf = ps.read_json(f'gs://data-lake-henry/metadata_{i}.json')[['gmap_id','name','address','avg_rating','num_of_reviews','price']]
#         logging.info(f'Cargo archivo metadata_{i}')
#         # psdf = sdf.pandas_api()
#         maps_metadata = ps.concat([maps_metadata,psdf])
#     except AnalysisException:
#         break

# logging.info(f'Archivos de metadata concatenados')

# maps_metadata = maps_metadata.reset_index(drop=True)
# for i in range(len(maps_metadata)):
#     if type(maps_metadata.name[i]) != str:
#         lista_metadata.append(i)
# maps_metadata.drop(lista_metadata,axis=0,inplace=True)
# horarios_maps = maps_metadata[['gmap_id','hours']]
# categorias_maps = maps_metadata[['gmap_id','category']]
# servicios_maps = maps_metadata[['gmap_id','MISC']]
# relativos_maps = maps_metadata[['gmap_id','relative_results']]
# maps_metadata = maps_metadata[['gmap_id','name','address','avg_rating','num_of_reviews','price']]

# logging.info(f'Pequeñas transformaciones')









# Concatenamos los diferentes archivos de reviews que tenemos para cada estado.
psdfx = ps.DataFrame()
for state in STATES:
    i = 1
    df_list = []
    while True:
        try:
            # Leemos los archivos en un SPARK Data Frame para poder acceder directamente a GCS
            sdf = spk.read.schema(schema).json(f'gs://data-lake-henry/{state}_{i}.json',multiLine=True,mode='DROPMALFORMED')[['user_id','name','time','rating','text','resp','gmap_id']]
            # PANDAS API Data Frame: Paso intermedio para trabajar con los métodos de pandas pero con la potencia de spark, posteriormente guardaremos los datos en BQ después de 
            # las transformaciones...
            psdf = sdf.pandas_api()
            psdf['estado'] = state
            df_list.append(psdf)
            i += 1
        except AnalysisException:
            break

    psdfx = ps.concat(df_list,axis=0)

    # Generamos el primer grupo de transformaciones para los datos de las reviews de Maps en PANDAS API. Queda la metadata y los archivos de Yelp.
    psdfx['resp_time'] = ps.Series(dtype=LongType)
    psdfx['resp_text'] = ps.Series(dtype=LongType)
    for i in range(len(psdfx)):
        if type(psdfx.resp[i]) == dict:
            psdfx.loc[i,'resp_time'] = psdfx.resp[i]['time']
            psdfx.loc[i,'resp_text'] = psdfx.resp[i]['text']
        else:
            psdfx.loc[i,'resp_time'] = 1
            psdfx.loc[i,'resp_text'] = ''
    psdfx.resp_time = psdfx.resp_time.fillna(0).astype(LongType)
    psdfx.resp_text = psdfx.resp_text.fillna('')
    psdfx = psdfx[['gmap_id','user_id','name','time','text','rating','resp_time','resp_text']]

    # Convertimos el dataframe de Pandas API on Spark a un dataframe de Spark
    sdf = psdf.to_spark()

    # Guardamos las tablas concatenadas en archivos .json en GCS.
    sdf.write.mode('overwrite').format('csv').save(f'gs://dataproc-pyspark-ops/out_dataproc/staging/all_{state}_raw')
