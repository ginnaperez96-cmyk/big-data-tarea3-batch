# Tarea 3 - Procesamiento en Batch con PySpark (UNAD)

## Descripción de la solución
Esta aplicación implementa un procesamiento por lotes (*Batch*) utilizando Apache Spark para la limpieza, transformación y Análisis Exploratorio de Datos (EDA) sobre un conjunto de datos masivo de COVID-19 de Colombia. El script procesa millones de registros, calcula métricas clave como el top de departamentos con más casos y la distribución por sexo de manera eficiente.

## Instrucciones de ejecución
1. Asegúrese de contar con un entorno Linux con Apache Spark instalado y el archivo del dataset (`dataset.csv`) ubicado en el directorio de trabajo.
2. Ejecute el script en la terminal mediante el siguiente comando:
   ```bash
   spark-submit spark_batch_analysis.py
