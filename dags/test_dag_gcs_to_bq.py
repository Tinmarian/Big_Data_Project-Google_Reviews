from airflow.decorators import dag, task_group

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyDatasetOperator

from datetime import datetime, timedelta


default_args = {
    "owner" : 'Tinmar Andrade',
		'start_date':datetime(2023,9,30),
        'schedule' : None,
		'email':['tinmar96@gmail.com','jozz.rom@gmail.com'],
		'email_on_failure':True
}

MY_BUCKET = 'data-lake-henry'
# MY_SOURCE_OBJECTS = ['Texas_6.json']
# MY_DESTINATION_PROJECT_DATASET_TABLE = 'pruebas_fph.dcs_review_texas' # final project henry data cleaning stage (pfh_dcs)
DATASET_PRUEBA = 'pruebas_fph'
DESTINATION_TABLE = 'dcs_review_texas'
GCPCONN = "google_cloud_henry"
PROJECT = 'fiery-protocol-399500'

# def listar():
#     list_files = []
#     for i in range(1,17):
#         x = f'Texas_{i}.json'
#         list_files.append(x)

MY_SOURCE_OBJECTS = ['1_test.ndjson']
                        # 'Texas_1.json',
                        # '1_test.json',
                        # 'Texas_2.json',
                        # 'Texas_3.json',
                        # 'Texas_4.json',
                        # 'Texas_5.json',
                        # 'Texas_6.json',
                        # 'Texas_7.json',
                        # 'Texas_8.json',
                        # 'Texas_9.json',
                        # 'Texas_10.json',
                        # 'Texas_11.json',
                        # 'Texas_12.json',
                        # 'Texas_13.json',
                        # 'Texas_14.json',
                        # 'Texas_15.json',
                        # 'Texas_16.json']     Se puede generar una tabla con varios archivos que contengan el mismo "schema".

@dag(
    'test_gcs_to_bq',
    default_args = default_args,
    catchup = False,
    tags = ['Prueba','GCS a BQ']
)

def test_gcs_to_bq():
    
    # Creamos el dataset donde vamos a almacenar los datos. Primero crearemos un dataset para hacer pruebas


    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset",
                                                        # dataset_id=f'{PROJECT}.{DATASET_PRUEBA}',
                                                        gcp_conn_id=GCPCONN,
                                                        dataset_id=DATASET_PRUEBA,
                                                        project_id=PROJECT,
                                                        location='us-east1'
                                                    )

    # Tabla de las Reviews de Texas.
    @task_group(
        group_id = 'TEXAS'
    )
    def tg1():
        for OBJECT in MY_SOURCE_OBJECTS:
            gcs_to_bq_prueba = GCSToBigQueryOperator(
                                                    task_id = f'prueba_{OBJECT}',
                                                    bucket=MY_BUCKET,
                                                    source_objects=OBJECT,
                                                    destination_project_dataset_table= f'{DATASET_PRUEBA}.{DESTINATION_TABLE}',# MY_DESTINATION_PROJECT_DATASET_TABLE,
                                                    schema_fields=[{'name':'user_id','type':'INT64','mode':'REQUIRED'}, 
                                                                    {'name':'name','type':'STRING','mode':'NULLABLE'}, 
                                                                    {'name':'time','type':'INT64','mode':'REQUIRED'}, 
                                                                    {'name':'rating','type':'INT64','mode':'NULLABLE'}, 
                                                                    {'name':'text','type':'STRING','mode':'NULLABLE'}, 
                                                                    {'name':'gmap_id','type':'STRING','mode':'REQUIRED'}, 
                                                                    {'name':'resp_time','type':'INT64','mode':'NULLABLE'}, 
                                                                    {'name':'resp_text','type':'STRING','mode':'NULLABLE'}
                                                                ],
                                                    autodetect=True,
                                                    source_format="NEWLINE_DELIMITED_JSON",
                                                    create_disposition="CREATE_IF_NEEDED",
                                                    skip_leading_rows="None",
                                                    write_disposition="WRITE_TRUNCATE", # WRITE_APPEND,
                                                    field_delimiter=",",
                                                    encoding="UTF-8",
                                                    gcp_conn_id=GCPCONN,
                                                    location="us-east1",
                                                    job_id="prueba_texas"
                                                )

    create_dataset >> tg1()

dag = test_gcs_to_bq()