import pandas as pd
# import numpy as np
import os
# import gcsfs


from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import IntegerType,StructField,StructType,StringType,LongType,MapType
import pyspark.pandas as ps

os.environ["PYARROW_IGNORE_TIMEZONE"] = '1' 
PROJECT_ID = 'fiery-protocol-399500'
STATES = ['California','Texas'] # ,'New_York','Colorado','Georgia']
schema = StructType([
    StructField('user_id',StringType(),False),
    StructField('name',StringType(),True),
    StructField('time',LongType(),True),
    StructField('rating',IntegerType(),True),
    StructField('text',StringType(),True),
    StructField('resp',MapType(StringType(),StringType()),True),
    StructField('gmap_id',StringType(),False)
])
# Create a SparkSession
spk = SparkSession.builder.appName("PySpark Transformations to Populate our Data Warehouse").getOrCreate()
ps.options.compute.ops_on_diff_frames = True


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
psdfx = ps.DataFrame(columns=['gmap_id','user_id','name','time','text','rating','resp_time','resp_text'])
for state in STATES:
    i = 1
    df_list = []
    while True:
        try:
            # Leemos los archivos en un SPARK Data Frame para poder acceder directamente a GCS
            sdf = spk.read.schema(schema).json(f'gs://data-lake-henry/{state}_{i}.json')[['user_id','name','time','rating','text','resp','gmap_id']]
            sdf.selectExpr('cast(user_id as int) user_id')
            # PANDAS API Data Frame: Paso intermedio para trabajar con los métodos de pandas pero con la potencia de spark, posteriormente guardaremos los datos en BQ después de 
            # las transformaciones...
            # sdf.count()
            psdf = sdf.pandas_api()
            psdf['time'] = ps.to_datetime(psdf['time'],unit='ms')            
            psdf['estado'] = state
            df_list.append(psdf)
            i += 1
        except AnalysisException:
            break

    psdf = ps.concat(df_list,axis=0)
    psdf.spark.persist()

    # Generamos el primer grupo de transformaciones para los datos de las reviews de Maps en PANDAS API. Queda la metadata y los archivos de Yelp.
    psdf['resp_time'] = ps.Series([],dtype='int64')
    psdf['resp_text'] = ps.Series([],dtype='str')
    # psdfx.astype({'resp_time' : 'int64'})
    # psdfx.astype({'resp_time' : str})
    for i in range(len(psdf)):
        if type(psdf.resp[i]) == type(dict):
            psdf.loc[i,'resp_time'] = psdf.resp[i]['time']
            psdf.loc[i,'resp_text'] = psdf.resp[i]['text']
        else:
            psdf.loc[i,'resp_time'] = 0
            psdf.loc[i,'resp_text'] = ''
    psdf.resp_time = psdf.resp_time.fillna(0).astype(LongType)
    psdf.resp_text = psdf.resp_text.fillna('')
    psdf = psdf[['gmap_id','user_id','name','time','text','rating','resp_time','resp_text']]

    # Aquí concatenamos todos los archivos del estado en curso a los demás estados, para obtener una tabla total de estados.
    psdfx = ps.concat(psdf,axis=0)

# Convertimos el dataframe de Pandas API on Spark a un dataframe de Spark
sdf = psdf.to_spark()

# Guardamos las tablas concatenadas en archivos .json en GCS.
sdf.write.mode('overwrite').format('csv').save(f'gs://dataproc-pyspark-ops/out_dataproc/staging/table_of_states')
