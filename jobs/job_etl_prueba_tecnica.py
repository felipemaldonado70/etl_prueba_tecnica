# -- coding: utf-8 --
""" Este modulo contiene la logica que lee los reportes de facturacion y los transforma para guardarlos
    en bigquery para su correspondiente analisis
"""
 
_author_ = "Luis Felipe Maldonado"
_maintainer_ = "Equipo Desarrollo"
_docformat_ = "Google"


from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col 
from pyspark.sql.functions import to_date
from pyspark.sql.functions import lit
from pyspark.sql.types import DoubleType


if __name__ == "__main__":
    try:
        # Inicializar la sesión de Spark
        spark = SparkSession.builder.appName("ETL_Facturacion_Cloud").getOrCreate()

        # constantes
        FECHA_ACTUAL = datetime.datetime.now().strftime("%Y%m%d")
        GCS_BUCKET = "gs://almacenamiento_etl_prueba/archivos_reporte/"
        RUTA_ARCHIVOS = GCS_BUCKET + FECHA_ACTUAL + "/"

        # leer archivos
        aws_data = spark.read.option("header", "true").csv(f"{RUTA_ARCHIVOS}/aws_facturacion_{fecha_actual}.csv")
        oci_data = spark.read.option("header", "true").csv(f"{ruta_archivos}/oci_fact_{fecha_actual}.csv")
        gcp_data = spark.read.option("header", "true").csv(f"{ruta_archivos}/gcp_costos_diarios_{fecha_actual}.csv")


        

    except Exception as e:
        raise f"Error inesperado {e}"