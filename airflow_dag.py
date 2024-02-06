from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import polars as pl
from ptransform import eliminar_columnas, explode_columna, eliminar_duplicados, filtrar_por_categoria
import json

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mi_dag',
    default_args=default_args,
    description='DAG para manipulación de datos en GCS con Polars',
    schedule_interval=timedelta(days=1),
)

# Tarea 1: Descargar archivo JSON desde GCS y cargar como DataFrame de Polars
def descargar_y_cargar_df(**kwargs):
    # Descargar archivo JSON
    download_gcs_task = GCSToLocalFilesystemOperator(
        task_id='download_gcs_file',
        bucket_name='your_gcs_bucket',
        object_name='your_json_file.json',
        filename='/tmp/input_file.json',
    )
    download_gcs_task.execute(context=kwargs)

    # Leer el archivo JSON y cargar como DataFrame de Polars
    with open('/tmp/input_file.json', 'r') as file:
        data = json.load(file)
    df = pl.DataFrame(data)

    # Almacenar el DataFrame resultante en el contexto de XCom para visualización
    kwargs['ti'].xcom_push(key='df_resultante_tarea1', value=df)

# Utilizamos PythonOperator para la tarea 1, ya que esta no necesita retornar nada
descargar_y_cargar_df_task = PythonOperator(
    task_id='descargar_y_cargar_df',
    python_callable=descargar_y_cargar_df,
    provide_context=True,
    dag=dag,
)

# Tarea 2: Realizar Transformacion 1 - Eliminar columnas
def realizar_transformacion_1(**kwargs):
    ti = kwargs['ti']
    # Obtener el DataFrame del contexto de XCom
    df = ti.xcom_pull(task_ids='descargar_y_cargar_df', key='df_resultante_tarea1')
    df_resultante = eliminar_columnas(df)

    # Almacenar el DataFrame resultante en el contexto de XCom para visualización
    ti.xcom_push(key='df_resultante_tarea2', value=df_resultante)

# Utilizamos PythonOperator para la tarea 2, ya que esta no necesita retornar nada
realizar_transformacion_1_task = PythonOperator(
    task_id='realizar_transformacion_1',
    python_callable=realizar_transformacion_1,
    provide_context=True,
    dag=dag,
)

# Tarea 3: Realizar Transformacion 2 - Explode columna 'category'
def realizar_transformacion_2(**kwargs):
    ti = kwargs['ti']
    # Obtener el DataFrame del contexto de XCom
    df = ti.xcom_pull(task_ids='realizar_transformacion_1', key='df_resultante_tarea2')
    df_resultante = explode_columna(df, 'category')

    # Almacenar el DataFrame resultante en el contexto de XCom para visualización
    ti.xcom_push(key='df_resultante_tarea3', value=df_resultante)

# Utilizamos PythonOperator para la tarea 3, ya que esta no necesita retornar nada
realizar_transformacion_2_task = PythonOperator(
    task_id='realizar_transformacion_2',
    python_callable=realizar_transformacion_2,
    provide_context=True,
    dag=dag,
)

# Tarea 4: Realizar Transformacion 3 - Eliminar duplicados
def realizar_transformacion_3(**kwargs):
    ti = kwargs['ti']
    # Obtener el DataFrame del contexto de XCom
    df = ti.xcom_pull(task_ids='realizar_transformacion_2', key='df_resultante_tarea3')
    df_resultante = eliminar_duplicados(df, 'name')

    # Almacenar el DataFrame resultante en el contexto de XCom para visualización
    ti.xcom_push(key='df_resultante_tarea4', value=df_resultante)

# Utilizamos PythonOperator para la tarea 4, ya que esta no necesita retornar nada
realizar_transformacion_3_task = PythonOperator(
    task_id='realizar_transformacion_3',
    python_callable=realizar_transformacion_3,
    provide_context=True,
    dag=dag,
)

# Tarea 5: Realizar Transformacion 4 - Filtrar por 'hotel' en la columna 'category'
def realizar_transformacion_4(**kwargs):
    ti = kwargs['ti']
    # Obtener el DataFrame del contexto de XCom
    df = ti.xcom_pull(task_ids='realizar_transformacion_3', key='df_resultante_tarea4')
    df_resultante = filtrar_por_categoria(df, 'category', 'hotel')

    # Almacenar el DataFrame resultante en el contexto de XCom para visualización
    ti.xcom_push(key='df_resultante_tarea5', value=df_resultante)

# Utilizamos PythonOperator para la tarea 5, ya que esta no necesita retornar nada
realizar_transformacion_4_task = PythonOperator(
    task_id='realizar_transformacion_4',
    python_callable=realizar_transformacion_4,
    provide_context=True,
    dag=dag,
)

# Tarea 6: Escribir resultado de Transformacion 4 en Parquet a GCS
def escribir_parquet_a_gcs(**kwargs):
    ti = kwargs['ti']
    # Obtener el DataFrame del contexto de XCom
    df = ti.xcom_pull(task_ids='realizar_transformacion_4', key='df_resultante_tarea5')

    # Escribir DataFrame en formato Parquet a GCS
    df.write_parquet('/tmp/output_parquet.parquet')
    upload_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_parquet_to_gcs',
        src='/tmp/output_parquet.parquet',
        dst='your_gcs_bucket/output_parquet.parquet',
    )
    upload_gcs_task.execute(context=kwargs)

# Utilizamos PythonOperator para la tarea 6, ya que esta no necesita retornar nada
escribir_parquet_a_gcs_task = PythonOperator(
    task_id='escribir_parquet_a_gcs',
    python_callable=escribir_parquet_a_gcs,
    provide_context=True,
    dag=dag,
)

# Configuración de dependencias entre tareas
descargar_y_cargar_df_task >> realizar_transformacion_1_task >> realizar_transformacion_2_task >> realizar_transformacion_3_task >> realizar_transformacion_4_task >> escribir_parquet_a_gcs_task
