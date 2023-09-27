from airflow.decorators import dag

from datetime import datetime

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

	MY_FOLDER_ID = '1y1XFty1aIIvAix3F8qpJMqnC_YiTIXyP'
# Probando funcionalidad
	prueba1 = GoogleDriveToGCSOperator(
		task_id = 'prueba1',
		bucket_name=MY_BUCKET_NAME,
		object_name='prueba1.docx',
		file_name='Diccionario de datos.docx',
		folder_id=MY_FOLDER_ID,
		# drive_id=SDRIVE_ID,
		gcp_conn_id=GCPCONN
	)

	# SDRIVE_ID = '0AOid89EWziepUk9PVA'
	prueba2 = GoogleDriveToGCSOperator(
		task_id = 'prueba2',
		bucket_name=MY_BUCKET_NAME,
		object_name='prueba2',
		file_name='prueba',
		folder_id=MY_FOLDER_ID,
		# drive_id=SDRIVE_ID,
		gcp_conn_id=GCPCONN
	)

# Carga de datos formal
	# Carga de datos Yelp
	MY_FOLDER_ID = '1TI-SsMnZsNP6t930olEEWbBQdo_yuIZF' # Folder de Yelp
	for MY_FILE_NAME in ['user.parquet','tip.json','review.json','business.pkl']:
		extract_load_yelp = GoogleDriveToGCSOperator(
			task_id = f'extract_load_yelp_{MY_FILE_NAME}',
			bucket_name=MY_BUCKET_NAME,
			object_name=MY_FILE_NAME,
			file_name=MY_FILE_NAME,
			folder_id=MY_FOLDER_ID,
			drive_id=MY_FOLDER_ID,
			gcp_conn_id=GCPCONN,
			trigger_rule='all_success'
		)

	# Cargar datos de Maps Metadata
	MY_FOLDER_ID = '1olnuKLjT8W2QnCUUwh8uDuTTKVZyxQ0Z'
	for MY_FILE_NAME in range(1,12):
		extract_load_maps_meta = GoogleDriveToGCSOperator(
			task_id = f'extract_load_maps_meta_{MY_FILE_NAME}',
			bucket_name=MY_BUCKET_NAME,
			object_name=f'metadata_{MY_FILE_NAME}.json',
			file_name=f'{MY_FILE_NAME}.json',
			folder_id=MY_FOLDER_ID,
			# drive_id=MY_FOLDER_ID,
			gcp_conn_id=GCPCONN,
			trigger_rule='all_success'
		)

	# Cargar datos de Maps Estados
	for OBJECT_NAME,MY_FOLDER_ID in zip(['review-New_York','review-California','review-Texas','review-Colorado','review-Georgia'],['18HYLDXcKg-cC1CT9vkRUCgea04cNpV33','1Jrbjt-0hnLCvecfrnMwGu1jYZSxElJll','1zq12pojMW2zeGgts0lHFSf_pF1L_4UWr','1IlUZJZxOyRiIWo3G6BqW1Thu2kXYKMFX','1MuPznes6CebS6gyWPVU-kR4EVKKLY4l3']):
		if OBJECT_NAME == 'review-New_York':
			for MY_FILE_NAME in range(1,19):
				extract_load_maps_newyork = GoogleDriveToGCSOperator(
					task_id = f'extract_load_maps_newyork_{MY_FILE_NAME}',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					# drive_id=MY_FOLDER_ID,
					gcp_conn_id=GCPCONN,
					trigger_rule='all_success'
				)

		if OBJECT_NAME == 'review-California':
			for MY_FILE_NAME in range(1,19):
				extract_load_maps_california = GoogleDriveToGCSOperator(
					task_id = f'extract_load_maps_california_{MY_FILE_NAME}',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					# drive_id=MY_FOLDER_ID,
					gcp_conn_id=GCPCONN,
					trigger_rule='all_success'
				)

		if OBJECT_NAME == 'review-Texas':
			for MY_FILE_NAME in range(1,17):
				extract_load_maps_texas = GoogleDriveToGCSOperator(
					task_id = f'extract_load_maps_texas_{MY_FILE_NAME}',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					# drive_id=MY_FOLDER_ID,
					gcp_conn_id=GCPCONN,
					trigger_rule='all_success'
					)

		if OBJECT_NAME == 'review-Colorado':
			for MY_FILE_NAME in range(1,17):
				extract_load_maps_colorado = GoogleDriveToGCSOperator(
					task_id = f'extract_load_maps_colorado_{MY_FILE_NAME}',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					# drive_id=MY_FOLDER_ID,
					gcp_conn_id=GCPCONN,
					trigger_rule='all_success'
				)

		if OBJECT_NAME == 'review-Georgia':
			for MY_FILE_NAME in range(1,14):
				extract_load_maps_georgia = GoogleDriveToGCSOperator(
					task_id = f'extract_load_maps_georgia_{MY_FILE_NAME}',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					# drive_id=MY_FOLDER_ID,
					gcp_conn_id=GCPCONN,
					trigger_rule='all_success'
				)

	delete_bucket >> create_bucket >> [prueba1,prueba2] >> extract_load_yelp >> extract_load_maps_meta >> [extract_load_maps_newyork, extract_load_maps_california, extract_load_maps_texas, extract_load_maps_colorado, extract_load_maps_georgia]

dag = gd_to_gcs()