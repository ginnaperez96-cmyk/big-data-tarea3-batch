from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc

def main():
    # 1. Crear la sesión de Spark para procesamiento Batch
    spark = SparkSession.builder \
        .appName("AnalisisBatchDatosAbiertosUNAD") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Cargar el dataset en formato CSV
    file_path = "dataset.csv"
    print("\n==============================================")
    print("--- 1. CARGANDO DATASET DE DATOS ABIERTOS ---")
    print("==============================================\n")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

    # 3. Mostrar el esquema y las primeras filas
    print("--- ESQUEMA DE LOS DATOS ---")
    df.printSchema()

    print("\n--- PRIMERAS 5 FILAS (VISTA PREVIA) ---")
    df.show(5, truncate=False)

    # 4. Limpieza de datos (Conteo de filas iniciales y eliminación de nulos)
    total_filas_original = df.count()
    
    # Seleccionamos y limpiamos las columnas de interés
    df_cleaned = df.select(
        col("Nombre departamento").alias("departamento"),
        col("Nombre municipio").alias("municipio"),
        col("Edad").alias("edad"),
        col("Sexo").alias("sexo"),
        col("Estado").alias("estado")
    ).dropna()

    total_filas_limpias = df_cleaned.count()

    print(f"\n--- LIMPIEZA DE DATOS ---")
    print(f"Total registros originales: {total_filas_original}")
    print(f"Total registros tras eliminar nulos: {total_filas_limpias}")

    # 5. Transformaciones y Análisis Exploratorio de Datos (EDA)
    print("\n==============================================")
    print("--- 2. RESULTADOS DEL ANÁLISIS BATCH (EDA) ---")
    print("==============================================\n")

    # A. Conteo de casos por Departamento
    print("--> Top 10 Departamentos con mayor número de casos registrados:")
    casos_por_dep = df_cleaned.groupBy("departamento") \
        .agg(count("*").alias("total_casos"), avg("edad").alias("promedio_edad")) \
        .orderBy(desc("total_casos"))
    
    casos_por_dep.show(10, truncate=False)

    # B. Distribución por Sexo
    print("--> Distribución de casos según Sexo:")
    casos_por_sexo = df_cleaned.groupBy("sexo") \
        .agg(count("*").alias("total_casos"))
    
    casos_por_sexo.show()

    # 6. Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
