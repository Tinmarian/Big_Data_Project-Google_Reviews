from airflow.decorators import dag

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

from datetime import datetime




GCPCONN = "google_cloud_henry"
PROJECT_ID = 'fiery-protocol-399500'
CLUSTER_NAME = 'test-dataproc-henry'
REGION = 'us-east1'
MY_BUCKET_NAME = 'dataproc-pyspark-ops'
DATASET = 'staging_fph'
PYSPARK_URI = 'gs://dataproc-pyspark-ops/pyspark-jobs/cleaning-stage/cleaning_job.py'
CLUSTER_CONFIG = {
    "master_config" : {
        "machine_type_uri" : "e2-standard-2",
        "disk_config" : {"boot_disk_type" : "pd-standard", "boot_disk_size_gb" : 75}
    },
    "worker_config" : {
        "num_instances" : 4,
        "machine_type_uri" : "c3-highcpu-4",
        "disk_config" : {"boot_disk_type" : "pd-balanced", "boot_disk_size_gb" : 75}
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
    'gcs_to_dataproc_cleaning_to_bq',
    catchup=False,
    default_args=default_args,
    tags = ['Prueba','GCS a DATAPROC','Transformaciones','HENRY']
)

def gcs_to_dataproc():

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset",
                                                        gcp_conn_id=GCPCONN,
                                                        dataset_id=DATASET,
                                                        project_id=PROJECT_ID,
                                                        location=REGION
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


    GCS_to_BQ = GCSToBigQueryOperator(
                                        task_id = f'gcs_to_bq_after_cleaning_reviews',
                                        bucket=MY_BUCKET_NAME,
                                        source_objects=[f'out_dataproc/staging/table_of_states/*.csv'],
                                        destination_project_dataset_table= f'{DATASET}.dcs_reviews_all_states',
                                        schema_fields=[{'name':'user_id','type':'INT64','mode':'REQUIRED'}, 
                                                        {'name':'name','type':'STRING','mode':'NULLABLE'}, 
                                                        {'name':'time','type':'INT64','mode':'REQUIRED'}, 
                                                        {'name':'rating','type':'INT32','mode':'NULLABLE'}, 
                                                        {'name':'text','type':'STRING','mode':'NULLABLE'}, 
                                                        {'name':'gmap_id','type':'STRING','mode':'REQUIRED'}, 
                                                        {'name':'resp_time','type':'INT64','mode':'NULLABLE'}, 
                                                        {'name':'resp_text','type':'STRING','mode':'NULLABLE'}
                                                    ],
                                        autodetect=True,
                                        source_format="CSV",
                                        create_disposition="CREATE_IF_NEEDED",
                                        skip_leading_rows="None",
                                        write_disposition="WRITE_TRUNCATE", # WRITE_APPEND,
                                        field_delimiter=",",
                                        encoding="UTF-8",
                                        gcp_conn_id=GCPCONN,
                                        location=REGION,
                                        job_id=f"ETL-gcs_to_dataproc_to_bq_states"
                                    )

    delete_cluster = DataprocDeleteClusterOperator(
                                                    task_id='delete_dataproc_cluster',
                                                    project_id=PROJECT_ID,
                                                    region=REGION,
                                                    cluster_name=CLUSTER_NAME,
                                                    gcp_conn_id=GCPCONN,
                                                    trigger_rule='all_done'
                                                )

    delete_staging_tables = GCSDeleteObjectsOperator(
                                                task_id='delete_output_folder_pyspark_job',
                                                gcp_conn_id=GCPCONN,
                                                bucket_name=MY_BUCKET_NAME,
                                                prefix='out_dataproc/staging/',
                                                objects=['*'],
                                                trigger_rule='all_done'
                                            )

    create_dataset >> create_cluster >> job >> GCS_to_BQ >> [delete_staging_tables,delete_cluster] 


dag = gcs_to_dataproc()