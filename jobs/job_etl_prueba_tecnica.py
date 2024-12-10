# -- coding: utf-8 --
""" Este modulo contiene la logica que lee los reportes de facturacion y los transforma para guardarlos
    en bigquery para su correspondiente analisis
"""

_author_ = "Luis Felipe Maldonado"
_maintainer_ = "Equipo Desarrollo"
_docformat_ = "Google"


from datetime import datetime
from datetime import timedelta
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
        SERVICIOS_CLOUD = {
            "AWS": {
                "Elastic Compute Cloud": {"categoria": "Cómputo"},
                "Virtual Private Cloud": {"categoria": "Red"},
                "CloudWatch": {"categoria": "Monitoreo"},
                "Glue": {"categoria": "Procesamiento de Datos"},
                "Simple Storage Service": {"categoria": "Almacenamiento"},
            },
            "OCI": {
                "Oracle Cloud Compute": {"categoria": "Cómputo"},
                "Oracle Cloud Virtual Cloud Network (VCN)": {"categoria": "Red"},
                "Oracle Cloud Monitoring and Logging": {"categoria": "Monitoreo"},
                "Oracle Cloud Data Integration": {
                    "categoria": "Procesamiento de Datos"
                },
                "Oracle Cloud Object Storage": {"categoria": "Almacenamiento"},
            },
            "GCP": {
                "Google Compute Engine (GCE)": {"categoria": "Cómputo"},
                "Google Virtual Private Cloud": {"categoria": "Red"},
                "Google Cloud Operations Suite": {"categoria": "Monitoreo"},
                "Google Cloud DataProc": {"categoria": "Procesamiento de Datos"},
                "Google Cloud Storage (GCS)": {"categoria": "Almacenamiento"},
            },
        }
        PROJECT_ID = "etlprueba"
        DATASET_ID = "facturacion"
        TABLE_NAME = "reporte_costos_proveedores_cloud"

        # Fecha límite: 15 días antes de la fecha actual
        FECHA_LIMITE = datetime.now() - timedelta(days=15)
        FECHA_LIMITE_STR = FECHA_LIMITE.strftime('%Y%m%d')

        # leer archivos
        aws_data = spark.read.option("header", "true").csv(
            f"{RUTA_ARCHIVOS}/aws_facturacion_{FECHA_ACTUAL}.csv"
        )
        oci_data = spark.read.option("header", "true").csv(
            f"{ruta_archivos}/oci_fact_{FECHA_ACTUAL}.csv"
        )
        gcp_data = spark.read.option("header", "true").csv(
            f"{ruta_archivos}/gcp_costos_diarios_{FECHA_ACTUAL}.csv"
        )

        # seleccion de columnas
        aws_data = (
            aws_data.select(
                col("account_id").alias("id_cuenta"),
                col("fecha").alias("fecha_reporte"),
                "servicio",
                col("importe_en_usd").alias("total"),
            )
            .withColumn("total_ARS", lit(0))
            .withColumn("nube", lit("AWS"))
            .withColumn("fecha", to_date(col("fecha"), "yyyyMMdd"))
        ).filter(col("fecha") >= FECHA_LIMITE)

        oci_data = (
            oci_data.select(
                col("account_id").alias("id_cuenta"),
                col("fecha").alias("fecha_reporte"),
                "servicio",
                "total",
            )
            .withColumn("total_ARS", col("total"))
            .withColumn("nube", lit("Oracle"))
            .withColumn("fecha", to_date(col("fecha"), "yyyyMMdd"))
        ).filter(col("fecha") >= FECHA_LIMITE)

        gcp_data = (
            gcp_data.select(
                col("account_id").alias("id_cuenta"),
                col("fecha").alias("fecha_reporte"),
                "servicio",
                col("importe_en_usd").alias("total"),
            )
            .withColumn("total_ARS", lit(0))
            .withColumn("nube", lit("GCP"))
            .withColumn("fecha", to_date(col("fecha"), "yyyyMMdd"))
        ).filter(col("fecha") >= FECHA_LIMITE)

        # transformar
        oci_data = oci_data.withColumn("total", col("total") / 500)

        # union dfs
        df = aws_data.union(oci_data).union(gcp_data)

        # creacion campo categoria de acuerdo al diccionario
        servicio_categoria = []
        for cloud, services in SERVICIOS_CLOUD.items():
            for service, details in services.items():
                servicio_categoria.append((service, details["categoria"]))

        # creacion dataframe con las categorías
        categoria_df = spark.createDataFrame(
            servicio_categoria, ["servicio", "categoria"]
        )

        # unir el dataframe de facturación con el dataframe de categorías
        df = df.join(categoria_df, on="servicio", how="left")

        # Si no se encuentra la categoría, asignar "NA"
        df = df.withColumn(
            "categoria",
            when(col("categoria").isNull(), lit("NA")).otherwise(col("categoria")),
        )

        # carga a tabla en bigquery
        df.write.format('bigquery') \
            .option('table', f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}') \
            .option('writeDisposition', 'WRITE_APPEND') \
            .save()


    except Exception as e:
        raise f"Error inesperado {e}"
