import datetime
from datetime import datetime, timedelta
from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago


# Definir los parámetros de tu clúster y proyecto

# Calcular la fecha de hoy en formato YYYYMMDD
now = datetime.now()
start_date = datetime(now.year, now.month, now.day, 9, 0)  # Día actual a las 9 AM
fecha_actual = datetime.now().strftime("%Y%m%d")
cluster_name = "cluster-d649"  # Nombre del clúster de Dataproc
project_id = "etlprueba"  # ID de tu proyecto en Google Cloud
region = "us-central1"  # Región de tu clúster (ajusta según tu región)

# Define el DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG(
    'etl_prueba_tecnica',
    default_args=default_args,
    description='ETL facturacion proveedores de nube',
    schedule_interval='0 9 * * *',
    start_date=start_date,
    tags=['prueba_tecnica'],
) as dag:
    pass