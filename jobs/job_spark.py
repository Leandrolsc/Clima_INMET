# -*- coding: utf-8 -*-

"""
Script para demonstrar como executar uma consulta SQL (SELECT) 
em arquivos Parquet usando um cluster Spark.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def create_spark_session():
    """Cria e retorna uma SparkSession."""
    return SparkSession.builder \
        .appName("SparkSQLonParquet") \
        .getOrCreate()

def query_parquet_data(spark, input_path):
    """
    Lê dados Parquet, registra como uma view temporária e executa uma consulta SQL.
    :param spark: SparkSession ativa.
    :param input_path: Caminho para o diretório com arquivos Parquet.
    """
    try:
        # 1. Lê os dados Parquet para um DataFrame
        print(f"Lendo dados Parquet de: {input_path}")
        df = spark.read.parquet(input_path)
        
        # 2. Cria uma "tabela" temporária na memória do Spark
        # Esta tabela só existe durante a execução desta aplicação.
        view_name = "vendas_analytics"
        df.createOrReplaceTempView(view_name)
        print(f"DataFrame registrado como a view temporária: '{view_name}'")

        # 3. Executa a consulta SQL usando spark.sql()
        # Esta consulta será distribuída e executada pelo cluster.
        print("Executando a consulta SQL...")
        query = f"""
            SELECT 
                *
            FROM {view_name}
            LIMIT 10
        """
        
        print("--- INÍCIO DA CONSULTA SQL ---")
        print(query.strip())
        print("--- FIM DA CONSULTA SQL ---")

        result_df = spark.sql(query)

        # 4. Mostra o resultado da consulta
        print("\nResultado da Consulta (Top 10 estados por faturamento em 2017):")
        result_df.show(truncate=False)

    except AnalysisException as e:
        print(f"Erro ao executar a consulta: {e}")
        print("Verifique se o caminho dos dados Parquet está correto e se o job de ETL foi executado antes.")
    finally:
        # Opcional: remove a view da memória
        spark.catalog.dropTempView(view_name)
        print(f"View temporária '{view_name}' removida.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: sql_query_script.py <caminho_dados_parquet>")
        sys.exit(-1)

    input_path_parquet = 'opt/bitnami/spark/app/arquivos/clima_inmet_aggregated.parquet'
    
    spark = create_spark_session()
    query_parquet_data(spark, input_path_parquet)
    spark.stop()


# /opt/bitnami/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   /opt/bitnami/spark/app/jobs/job_spark.py \
#   /opt/bitnami/spark/app/output/