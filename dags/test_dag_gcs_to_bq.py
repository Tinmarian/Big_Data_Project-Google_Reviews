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
# MY_DESTINATION_PROJECT_DATASET_TABLE = 'pruebas_fph.dcs_review_texas' # final project henry data cleaning stage (pfh_dcs)
DATASET_PRUEBA = 'pruebas_fph'
DESTINATION_TABLE = 'dcs_review_texas'
GCPCONN = "google_cloud_henry"

# def listar():
#     list_files = []
#     for i in range(1,17):
#         x = f'gs://data-lake-henry/Texas_{i}.json'
#         list_files.append(x)

MY_SOURCE_OBJECTS = ['gs://data-lake-henry/Texas_1.json',
                        'gs://data-lake-henry/Texas_2.json',
                        'gs://data-lake-henry/Texas_3.json',
                        'gs://data-lake-henry/Texas_4.json',
                        'gs://data-lake-henry/Texas_5.json',
                        'gs://data-lake-henry/Texas_6.json',
                        'gs://data-lake-henry/Texas_7.json',
                        'gs://data-lake-henry/Texas_8.json',
                        'gs://data-lake-henry/Texas_9.json',
                        'gs://data-lake-henry/Texas_10.json',
                        'gs://data-lake-henry/Texas_11.json',
                        'gs://data-lake-henry/Texas_12.json',
                        'gs://data-lake-henry/Texas_13.json',
                        'gs://data-lake-henry/Texas_14.json',
                        'gs://data-lake-henry/Texas_15.json',
                        'gs://data-lake-henry/Texas_16.json']

@dag(
    'gcs_to_bq',
    default_args = default_args,
    catchup = False,
    tags = ['HENRY','Proyecto Final','Proyecto en Equipo','Limpieza','Primeras Transformaciones']
)

def gcs_to_bq_limpieza():
    
    # Creamos el dataset donde vamos a almacenar los datos. Primero crearemos un dataset para hacer pruebas
    
        
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_PRUEBA)

    # Tabla de las Reviews de Texas.
    gcs_to_bq_prueba = GCSToBigQueryOperator(
    task_id = 'prueba',
    bucket=MY_BUCKET,
    source_objects=MY_SOURCE_OBJECTS,
    destination_project_dataset_table= f'{DATASET_PRUEBA}.{DESTINATION_TABLE}',# MY_DESTINATION_PROJECT_DATASET_TABLE,
    schema_fields="None",
    source_format="NEWLINE_DELIMITED_JSON",
    create_disposition="CREATE_IF_NEEDED",
    skip_leading_rows="None",
    write_disposition="WRITE_TRUNCATE", # WRITE_APPEND,
    field_delimiter=",",
    encoding="UTF-8",
    gcp_conn_id=GCPCONN,
    location="None",
    job_id="prueba_texas"
    )
    
    # # Tabla de las Reviews de New York.
    # gcs_to_bq = f = GCSToBigQueryOperator(
    # bucket=MY_BUCKET,
    # source_objects=MY_SOURCE_OBJECTS,
    # destination_project_dataset_table= f'{DATASET_PRUEBA}.{DESTINATION_TABLE}',# MY_DESTINATION_PROJECT_DATASET_TABLE,
    # schema_fields="None",
    # source_format="JSON",
    # create_disposition="CREATE_IF_NEEDED",
    # skip_leading_rows="None",
    # write_disposition="WRITE_TRUNCATE", # WRITE_APPEND,
    # field_delimiter=",",
    # encoding="UTF-8",
    # gcp_conn_id=GCPCONN,
    # location="None",
    # job_id="prueba_texas"
    # )
    
    # # Tabla de las Reviews de California.
    # gcs_to_bq = f = GCSToBigQueryOperator(
    # bucket=MY_BUCKET,
    # source_objects=MY_SOURCE_OBJECTS,
    # destination_project_dataset_table= f'{DATASET_PRUEBA}.{DESTINATION_TABLE}',# MY_DESTINATION_PROJECT_DATASET_TABLE,
    # schema_fields="None",
    # source_format="JSON",
    # create_disposition="CREATE_IF_NEEDED",
    # skip_leading_rows="None",
    # write_disposition="WRITE_TRUNCATE", # WRITE_APPEND,
    # field_delimiter=",",
    # encoding="UTF-8",
    # gcp_conn_id=GCPCONN,
    # location="None",
    # job_id="prueba_texas"
    # )
    
    # # Tabla de las Reviews de Colorado.
    # gcs_to_bq = f = GCSToBigQueryOperator(
    # bucket=MY_BUCKET,
    # source_objects=MY_SOURCE_OBJECTS,
    # destination_project_dataset_table= f'{DATASET_PRUEBA}.{DESTINATION_TABLE}',# MY_DESTINATION_PROJECT_DATASET_TABLE,
    # schema_fields="None",
    # source_format="JSON",
    # create_disposition="CREATE_IF_NEEDED",
    # skip_leading_rows="None",
    # write_disposition="WRITE_TRUNCATE", # WRITE_APPEND,
    # field_delimiter=",",
    # encoding="UTF-8",
    # gcp_conn_id=GCPCONN,
    # location="None",
    # job_id="prueba_texas"
    # )
    
    # # Tabla de las Reviews de Georgia.
    # gcs_to_bq = f = GCSToBigQueryOperator(
    # bucket=MY_BUCKET,
    # source_objects=MY_SOURCE_OBJECTS,
    # destination_project_dataset_table= f'{DATASET_PRUEBA}.{DESTINATION_TABLE}',# MY_DESTINATION_PROJECT_DATASET_TABLE,
    # schema_fields="None",
    # source_format="JSON",
    # create_disposition="CREATE_IF_NEEDED",
    # skip_leading_rows="None",
    # write_disposition="WRITE_TRUNCATE", # WRITE_APPEND,
    # field_delimiter=",",
    # encoding="UTF-8",
    # gcp_conn_id=GCPCONN,
    # location="None",
    # job_id="prueba_texas"
    # )
    
    create_dataset >> gcs_to_bq_prueba
    
dag = gcs_to_bq_limpieza()