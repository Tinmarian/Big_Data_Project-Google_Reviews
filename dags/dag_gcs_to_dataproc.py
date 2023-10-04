from airflow.decorators import dag,task,task_group

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyDatasetOperator


from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime, timedelta


GCPCONN = "google_cloud_henry"
PROJECT_ID = 'fiery-protocol-399500'
CLUSTER_NAME = 'test-dataproc-henry'
REGION = 'us-east1'
MY_BUCKET_NAME = 'dataproc-pyspark-ops'
DATASET = 'staging-fph'
STATES = sorted(['California','Texas','New_York','Colorado','Georgia'])



PYSPARK_URI = 'gs://data-lake-henry/pyspark-jobs/cleaning-stage/cleaning_job.py'
CLUSTER_CONFIG = {
    "master_config" : {
        "machine_type_uri" : "n2-standard-2",
        "disk_config" : {"boot_disk_type" : "pd-standard", "boot_disk_size_gb" : 75}
    },
    "worker_config" : {
        "num_instances" : 2,
        "machine_type_uri" : "e2-standard-8",
        "disk_config" : {"boot_disk_type" : "pd-standard", "boot_disk_size_gb" : 75}
    },
    "software_config" : {
        "image_version" : '2.1.25-ubuntu20'
    }
}

PYSPARK_JOB = {
    "reference" : {"project_id":PROJECT_ID},
    "placement" : {"cluster_name":CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri":PYSPARK_URI,}
}


default_args = {
    "owner" : 'Tinmar Andrade',
    'start_date':datetime(2023,9,30),
    'schedule' : None,
    'email':['tinmar96@gmail.com','jozz.rom@gmail.com'],
    'email_on_failure':True
}

@dag(
    'test_gcs_to_dataproc',
    catchup=False,
    default_args=default_args,
    tags = ['Prueba','GCS a DATAPROC','Transformaciones']
)

def test_gcs_to_dataproc():
    
    
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset",
                                                        gcp_conn_id=GCPCONN,
                                                        dataset_id=DATASET,
                                                        project_id=PROJECT_ID,
                                                        location='us-east1'
                                                    )


    create_cluster = DataprocCreateClusterOperator(
                                                    task_id='create_dataproc_cluster',
                                                    project_id=PROJECT_ID,
                                                    cluster_config=CLUSTER_CONFIG,
                                                    region=REGION,
                                                    cluster_name=CLUSTER_NAME,
                                                    gcp_conn_id=GCPCONN,
                                                    use_if_exists=True
                                                )


    job = DataprocSubmitJobOperator(
                                    task_id='pyspark_job',
                                    job=PYSPARK_JOB,
                                    project_id=PROJECT_ID,
                                    region=REGION,
                                    gcp_conn_id=GCPCONN,
                                    trigger_rule='all_success'
                                )


    @task(task_id='listing_gcs_json_files')
    def list_blobs(bucket_name,states):
        SCOPES = ['https://www.googleapis.com/auth/drive.readonly','https://www.googleapis.com/auth/devstorage.full_control','https://www.googleapis.com/auth/drive']
        SERVICE_ACCOUNT_FILE = './credentials/fiery-protocol-399500-f2566dd92ef4.json'
        
        CREDS = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        storage_client = storage.Client(credentials=CREDS)

        """Lists all the blobs in the bucket."""
        FOLDERS = []
        for state in states:
            blobs = storage_client.list_blobs(bucket_name, prefix=f'out_dataproc/all_{state}_raw')
    
        # Note: The call returns a response only when the iterator is consumed.
    
            URI = []
            for blob in blobs:
                if 'json' in blob.name:
                    URI.append(f'gs://{blob.id[:-17]}')
                    kwargs['ti'].xcom_push(key=state, value=URI)


    @task
    def printed():
        print(kwargs['ti'].xcom_pull(task_ids='listing_gcs_json_files',key=state))

    @task_group(group_id='unique_state_tables')
    def tg1(states):
        i = 0
        for state in states:
            URIs = kwargs['ti'].xcom_pull(task_ids='listing_gcs_json_files',key=state)[0]
            GCS_to_BQ = GCSToBigQueryOperator(
                                                task_id = f'gcs_to_bq_after_cleaning',
                                                bucket=MY_BUCKET_NAME,
                                                source_objects=URIs,
                                                destination_project_dataset_table= f'{DATASET}.dcs_review_{state}',
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
            i += 1



    delete_cluster = DataprocDeleteClusterOperator(
                                                    task_id='delete_dataproc_cluster',
                                                    project_id=PROJECT_ID,
                                                    region=REGION,
                                                    cluster_name=CLUSTER_NAME,
                                                    gcp_conn_id=GCPCONN,
                                                    trigger_rule='all_done'
                                                )


    create_cluster >> job >> list_blobs(MY_BUCKET_NAME,STATES) >> printed() >> tg1(STATES) >> delete_cluster


dag = test_gcs_to_dataproc()