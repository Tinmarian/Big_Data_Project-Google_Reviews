from airflow.decorators import dag, task, task_group

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyDatasetOperator

from datetime import datetime, timedelta


default_args = {
    "owner" : 'Tinmar Andrade',
		'start_date':datetime(2023,9,20),
        'schedule' : None,
		'email':['tinmar96@gmail.com','jozz.rom@gmail.com'],
		'email_on_failure':True
}

MY_BUCKET = 'data-lake-henry'
MY_SOURCE_OBJECTS = ['gs://data-lake-henry/Texas_6.json']
# MY_DESTINATION_PROJECT_DATASET_TABLE = 'pruebas_pfh.dcs_review_texas' # proyect final henry data cleaning stage (pfh_dcs)
DATASET_PRUEBA = 'pruebas_pfh'
DESTINATION_TABLE = 'dcs_review_texas'

@dag(
    'gcs_to_bq',
    default_args = default_args,
    catchup = False,
    tags = ['HENRY','Proyecto Final','Proyecto en Equipo']
)

def gcs_to_bq():
    
    # Creamos el dataset donde vamos a almacenar los datos. Primero crearemos un dataset para hacer pruebas
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_PRUEBA)

    gcs_to_bq = f = GCSToBigQueryOperator(
	bucket=MY_BUCKET,
	source_objects=MY_SOURCE_OBJECTS,
	destination_project_dataset_table= f'{DATASET_PRUEBA}.{DESTINATION_TABLE}',# MY_DESTINATION_PROJECT_DATASET_TABLE,
	schema_fields="None",
	source_format="JSON",
	create_disposition="CREATE_IF_NEEDED",
	skip_leading_rows="None",
	write_disposition="WRITE_TRUNCATE", # WRITE_APPEND,
	field_delimiter=",",
	encoding="UTF-8",
	gcp_conn_id="google_cloud_default",
	location="None",
	job_id="prueba_texas"
)