from airflow.decorators import dag, task_group

from datetime import datetime, timedelta

from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator

from google.oauth2 import service_account

# SCOPES = ['https://www.googleapis.com/auth/drive.readonly','https://www.googleapis.com/auth/devstorage.full_control','https://www.googleapis.com/auth/drive']
# SERVICE_ACCOUNT_FILE = '/c/Users/tinma/OneDrive/Escritorio/HENRY/Proyecto_Grupal_HENRY/credentials/fiery-protocol-399500-f2566dd92ef4.json'

# credentials = service_account.Credentials.from_service_account_file(
#         SERVICE_ACCOUNT_FILE, scopes=SCOPES)


GCPCONN = "google_cloud_henry"
MY_BUCKET_NAME = 'data-lake-henry'
HENRY_PROJECT = 'fiery-protocol-399500'

default_args = {
		'owner':'Tinmar Andrade',
		'start_date':datetime(2023,9,20),
		'email':['tinmar96@gmail.com','jozz.rom@gmail.com'],
		'email_on_failure':True,
	}

@dag(
	'gd_to_gcs',
	default_args=default_args,
	catchup=False,
	schedule=None,
	tags=['HENRY','Proyecto Final','Proyecto en Equipo']
	)

def gd_to_gcs():

	delete_bucket = GCSDeleteBucketOperator(
		task_id = 'delete_bucket',
		force = True,
		bucket_name = MY_BUCKET_NAME,
		gcp_conn_id = GCPCONN
	)

	create_bucket = GCSCreateBucketOperator(
		task_id = 'create_bucket',
		bucket_name = MY_BUCKET_NAME,
		location = 'us-east1',
		project_id = HENRY_PROJECT,
		storage_class = 'STANDARD',
		gcp_conn_id = GCPCONN
	)

	# Carga de datos Yelp
	@task_group(
		group_id="yelp_load"
	)
	def tg1():
		MY_FOLDER_ID = '1IcC0SiBY2UeyRUn7gcs2PfIhwptvDRdk' # Folder de Yelp
		for MY_FILE_NAME in ['user.parquet','tip.json','review.json','business.pkl']: # ,'checkin.json']:
			extract_load_yelp = GoogleDriveToGCSOperator(
				task_id = f'extract_load_yelp_{MY_FILE_NAME}',
				retries = 5,
				bucket_name=MY_BUCKET_NAME,
				object_name=MY_FILE_NAME,
				file_name=MY_FILE_NAME,
				folder_id=MY_FOLDER_ID,
				gcp_conn_id=GCPCONN,
				trigger_rule='all_done',
				execution_timeout=timedelta(seconds=60*45)
			)

	# Cargar datos de Maps Metadata
	@task_group(
		group_id="maps_meta_load"
	)
	def tg2():
		MY_FOLDER_ID = '1FrOLWoLdPDyi1h7BKT5z-LSGjuMxIyKb'
		for MY_FILE_NAME in range(1,12):
			extract_load_maps_meta = GoogleDriveToGCSOperator(
				task_id = f'extract_load_maps_meta_{MY_FILE_NAME}',
				retries = 5,
				bucket_name=MY_BUCKET_NAME,
				object_name=f'metadata_{MY_FILE_NAME}.json',
				file_name=f'{MY_FILE_NAME}.json',
				folder_id=MY_FOLDER_ID,
				gcp_conn_id=GCPCONN,
				trigger_rule='all_done',
				execution_timeout=timedelta(seconds=60*45)
			)

	# Cargar datos de Maps Estados
	OBJECT_NAME,MY_FOLDER_ID = ['review-New_York','review-California','review-Texas','review-Colorado','review-Georgia'],['1fNf7hKTn9W3XVtbeKN_w7G_eVVviFCR0','1ikEacrw5YaD3_yZQK7OOArN1VjGXJOhh','1u-q7_KBPQlKuICsjorm0nQR8CRhkImKc','13FmmDFGjcMacTm9bLEeq9yO7BESLprIK','1bB7j6_ojiSZSm7WYZ2Sue67qrShQNMit']

	@task_group(
		group_id="maps_ny_load"
	)
	def tg3():
		for MY_FILE_NAME in range(1,19):
			extract_load_maps_newyork = GoogleDriveToGCSOperator(
				task_id = f'extract_load_maps_newyork_{MY_FILE_NAME}',
				retries = 5,
				bucket_name=MY_BUCKET_NAME,
				object_name=f'{OBJECT_NAME[0][7:]}_{MY_FILE_NAME}.json',
				file_name=f'{MY_FILE_NAME}.json',
				folder_id=MY_FOLDER_ID[0],
				gcp_conn_id=GCPCONN,
				trigger_rule='all_done',
				execution_timeout=timedelta(seconds=60*45)
			)

	@task_group(
		group_id="maps_ca_load"
	)
	def tg4():
		for MY_FILE_NAME in range(1,19):
			extract_load_maps_california = GoogleDriveToGCSOperator(
				task_id = f'extract_load_maps_california_{MY_FILE_NAME}',
				retries = 5,
				bucket_name=MY_BUCKET_NAME,
				object_name=f'{OBJECT_NAME[1][7:]}_{MY_FILE_NAME}.json',
				file_name=f'{MY_FILE_NAME}.json',
				folder_id=MY_FOLDER_ID[1],
				gcp_conn_id=GCPCONN,
				trigger_rule='all_done',
				execution_timeout=timedelta(seconds=60*45)
			)

	@task_group(
		group_id="maps_tx_load"
	)
	def tg5():
		for MY_FILE_NAME in range(1,17):
			extract_load_maps_texas = GoogleDriveToGCSOperator(
				task_id = f'extract_load_maps_texas_{MY_FILE_NAME}',
				retries = 5,
				bucket_name=MY_BUCKET_NAME,
				object_name=f'{OBJECT_NAME[2][7:]}_{MY_FILE_NAME}.json',
				file_name=f'{MY_FILE_NAME}.json',
				folder_id=MY_FOLDER_ID[2],
				gcp_conn_id=GCPCONN,
				trigger_rule='all_done',
				execution_timeout=timedelta(seconds=60*45)
				)

	@task_group(
		group_id="maps_co_load"
	)
	def tg6():
		for MY_FILE_NAME in range(1,17):
			extract_load_maps_colorado = GoogleDriveToGCSOperator(
				task_id = f'extract_load_maps_colorado_{MY_FILE_NAME}',
				retries = 5,
				bucket_name=MY_BUCKET_NAME,
				object_name=f'{OBJECT_NAME[3][7:]}_{MY_FILE_NAME}.json',
				file_name=f'{MY_FILE_NAME}.json',
				folder_id=MY_FOLDER_ID[3],
				gcp_conn_id=GCPCONN,
				trigger_rule='all_done',
				execution_timeout=timedelta(seconds=60*45)
			)

	@task_group(
		group_id="maps_ge_load"
	)
	def tg7():
		for MY_FILE_NAME in range(1,14):
			extract_load_maps_georgia = GoogleDriveToGCSOperator(
				task_id = f'extract_load_maps_georgia_{MY_FILE_NAME}',
				retries = 5,
				bucket_name=MY_BUCKET_NAME,
				object_name=f'{OBJECT_NAME[4][7:]}_{MY_FILE_NAME}.json',
				file_name=f'{MY_FILE_NAME}.json',
				folder_id=MY_FOLDER_ID[4],
				gcp_conn_id=GCPCONN,
				trigger_rule='all_done',
				execution_timeout=timedelta(seconds=60*45)
			)

	delete_bucket >> create_bucket >> tg1() >> tg2() >> tg3() >> tg4() >> tg5() >> tg6() >> tg7()

dag = gd_to_gcs()