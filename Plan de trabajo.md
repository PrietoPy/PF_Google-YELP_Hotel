# Plan de Trabajo

## 1. Análisis y Diseño:

- Revisar los requisitos y entender la estructura de los datos en bruto.
- Diseñar el esquema de almacenamiento en Firestore y la estructura de los datos en formato Parquet.
- Definir las transformaciones necesarias para limpiar los datos.

## 2. Configuración de GCP:

- Crear un proyecto en Google Cloud Platform (GCP) si no existe.
- Configurar Cloud Storage (GCS) para almacenar los datos en bruto y los datos limpios en formato Parquet.

## 3. Configuración de Airflow:

- Instalar y configurar Apache Airflow en GCP.
- Crear un DAG principal que contenga todas las tareas de transformación.
- Configurar un trigger en otro DAG para activar la ejecución cuando nuevos archivos lleguen a GCS.

## 4. Implementación de Transformaciones:

- Desarrollar las transformaciones necesarias en Python utilizando operadores de Airflow.
- Asegurarse de que el DAG principal orqueste eficientemente las tareas de limpieza y transformación de datos.

## 5. Configuración de Firebase Firestore:

- Configurar una base de datos Firestore en modo nativo para almacenar los datos limpios.
- Implementar la lógica para insertar los datos limpios en Firestore después de la limpieza.

## 6. Monitoreo y Logging:

- Configurar registros y monitoreo en Airflow para realizar un seguimiento del rendimiento del DAG.
- Establecer alertas para cualquier anomalía o error durante la ejecución de las tareas.

# KPI de Proceso

## 1. Tiempo de Procesamiento:

- Medir el tiempo total que tarda el DAG principal en procesar los datos desde la llegada a GCS hasta la carga en Firestore.

## 2. Eficiencia de Transformación:

- Evaluar la eficiencia de las transformaciones mediante el análisis del tiempo dedicado a cada tarea dentro del DAG.

## 3. Disponibilidad del Sistema:

- Evaluar la disponibilidad del sistema y la ejecución del DAG a través del monitoreo constante.

# KPI de Rendimiento

## 1. Rendimiento de Firestore:

- Medir el tiempo de escritura en Firestore y la eficiencia del proceso de inserción de datos.

## 2. Consumo de Recursos:

- Evaluar el consumo de recursos en GCP, como el uso de CPU y memoria durante la ejecución del DAG.

## 3. Escalabilidad:

- Evaluar la capacidad del sistema para manejar volúmenes crecientes de datos sin degradación significativa del rendimiento.

# Diagrama de Gantt

## Febrero 2024

| Tarea                                             | 12-14 | 15-17 | 18-20 | 21-23 | 24-26 |
| ------------------------------------------------- | ----- | ----- | ----- | ----- | ----- |
| **Análisis y Diseño**                             | ✔️    | ✔️    |       |       |       |
| **Configuración de GCP**                          |       | ✔️    | ✔️    |       |       |
| **Configuración de Airflow**                      |       |       | ✔️    | ✔️    |       |
| **Implementación de Transformaciones**            |       |       |       | ✔️    | ✔️    |
| **Configuración de Firebase Firestore**           |       |       |       | ✔️    | ✔️    |
| **Monitoreo y Logging**                           |       |       |       | ✔️    | ✔️    |
| **KPI de Proceso - Tiempo de Procesamiento**      |       |       |       | ✔️    | ✔️    |
| **KPI de Proceso - Eficiencia de Transformación** |       |       |       | ✔️    | ✔️    |
| **KPI de Proceso - Disponibilidad del Sistema**   |       |       |       | ✔️    | ✔️    |
| **KPI de Rendimiento - Rendimiento de Firestore** |       |       |       |       | ✔️    |
| **KPI de Rendimiento - Consumo de Recursos**      |       |       |       |       | ✔️    |
| **KPI de Rendimiento - Escalabilidad**            |       |       |       |       | ✔️    |

Legend:

- ✔️: Tarea en progreso o completada.