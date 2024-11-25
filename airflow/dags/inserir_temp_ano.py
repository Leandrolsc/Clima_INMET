from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
# Função para inserir os dados no PostgreSQL usando uma Connection configurada
def inserir_dados():
    # Nome da conexão configurada no Airflow (Admin > Connections)
    conn_id = "Db"  # Substitua pelo ID da sua conexão
    table_name = "temperatura_ano"  # Nome da tabela no banco
    file_path = "/opt/airflow/arquivos/Temperatura_Ano.xlsx"
    #file_path = "C:/Users/leand/airflow-dbt/arquivos/Temperatura_Ano.xlsx"

    # Obtém a conexão do Airflow
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    # Lê o arquivo Excel
    df = pd.read_excel(file_path)
    if not os.path.exists(file_path):
        print(f"Arquivo não encontrado: {file_path}")
    else:
        print("Arquivo localizado com sucesso.")

    # Insere os dados no banco de dados
    try:
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        print(f"Dados inseridos com sucesso na tabela '{table_name}'.")
    except Exception as e:
        print("Erro ao inserir os dados:", e)
        raise
    finally:
        engine.dispose()

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='dag_inserir_dados_postgres',
    default_args=default_args,
    description='Insere dados do arquivo Excel no PostgreSQL usando Connection',
    schedule_interval=None,  # Execução manual
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['postgres', 'excel', 'ETL']
) as dag:
    inserir_dados_task = PythonOperator(
        task_id='inserir_dados',
        python_callable=inserir_dados
    )

    inserir_dados_task
