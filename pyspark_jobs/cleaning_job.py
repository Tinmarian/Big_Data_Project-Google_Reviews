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
    
    # Concatenamos los diferentes archivos que tenemos para cada estado.
    i = 1
    df_list = []
    psdfx = ps.DataFrame()
    while True:
        try:
            # Leemos los archivos en un SPARK Data Frame para poder acceder directamente a GCS
            sdf = spk.read.json(f'gs://data-lake-henry/{state}_{i}.json')
            # PANDAS API Data Frame: Paso intermedio para generar un PANDAS Data Frame.
            psdf = sdf.pandas_api()
            # Generamos las primeras transformaciones en PANDAS Data Frame
            pdf = psdf.to_pandas()
            pdf.user_id = pdf.user_id.astype(str)
            pdf.user_id = pdf.user_id.apply(lambda x: x.replace('e+20','').replace('.',''))
            pdf.user_id = pdf.user_id.apply(lambda x: int(x))
            pdf.name = pdf.name.astype(str)
            pdf.text = pdf.text.astype(str)
            pdf.pics = pdf.pics.astype(str)
            pdf.gmap_id = pdf.gmap_id.astype(str)
            # PANDAS API Data Frame
            psdf = ps.from_pandas(pdf)
            df_list.append(psdf)
            i += 1
        except AnalysisException:
            break
    psdfx = ps.concat(df_list,axis=0)
    
    # Generamos el segundo grupo de transformaciones a los datos de las reviews de Maps en PANDAS API. Queda la metadata y los archivos de Yelp.
    psdfx['resp_time'] = ps.Series()
    psdfx['resp_text'] = ps.Series()
    for i in range(len(psdfx)):
        if type(psdfx.resp[i]) == dict:
            psdfx.loc[i,'resp_time'] = psdfx.resp[i]['time']
            psdfx.loc[i,'resp_text'] = psdfx.resp[i]['text']
        else:
            psdfx.loc[i,'resp_time'] = 1
            psdfx.loc[i,'resp_text'] = ''
    psdfx.resp_time = psdfx.resp_time.fillna(0).apply(lambda x: int(x))
    psdfx.resp_text = psdfx.resp_text.fillna('')
    psdfx = psdfx[['user_id', 'name', 'time', 'rating', 'text', 'gmap_id', 'resp_time', 'resp_text']]

    # Convertimos el dataframe de Pandas API on Spark a un dataframe de Spark
    sdf = psdf.to_spark()

    # Guardamos las tablas concatenadas en archivos .json en GCS.
    sdf.write.mode('overwrite').format('csv').save(f'gs://dataproc-pyspark-ops/out_dataproc/staging/all_{state}_raw')
