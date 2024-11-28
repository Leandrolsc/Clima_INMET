from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
import pandas as pd
import os
import unidecode

# Função para listar arquivos
def listar_arquivos_e_pastas(caminho_base, **kwargs):
    arquivos = []
    if not os.path.exists(caminho_base):
        print(f"A pasta {caminho_base} não foi encontrada.")
        return arquivos
    
    for root, _, files in os.walk(caminho_base):
        for arquivo in files:
            caminho_arquivo = os.path.join(root, arquivo)
            if os.path.isfile(caminho_arquivo):
                arquivos.append(caminho_arquivo)
    # Salva a lista de arquivos como variável XCom
    kwargs['ti'].xcom_push(key='arquivos', value=arquivos)

# Função para processar arquivo
def processar_arquivo(arquivo_path):
    def convert_temperatura(value):
        if isinstance(value, str):
            return float(value.replace(',', '.'))
        elif isinstance(value, (float, int)):
            return float(value)
        return None

    print(f'Processando arquivo: {arquivo_path}')
    try:
        # Lê o arquivo com os dados
        arquivo = pd.read_csv(
            arquivo_path, sep=';', header=8, encoding='latin1'
        )
        colunas_arquivo = list(arquivo.columns)
        if 'DATA (YYYY-MM-DD)' in colunas_arquivo:
            colunas = ['DATA (YYYY-MM-DD)', 'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)']
            arquivo = arquivo[colunas]
            arquivo.rename(columns={
                'DATA (YYYY-MM-DD)': 'Data',
                'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'Temperatura'
            }, inplace=True)
        else:
            colunas = ['Data', 'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)']
            arquivo = arquivo[colunas]
            arquivo.rename(columns={
                'Data': 'Data',
                'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'Temperatura'
            }, inplace=True)

        arquivo['Temperatura'] = arquivo['Temperatura'].apply(convert_temperatura)
        arquivo = arquivo[arquivo['Temperatura'] != -9999]
        arquivo['Data'] = pd.to_datetime(arquivo['Data'])

        # Lê o cabeçalho do arquivo
        cabecalho = pd.read_csv(
            arquivo_path, delimiter=';', nrows=7, encoding='latin1', header=None
        )
        cabecalho[0] = cabecalho[0].apply(lambda x: unidecode.unidecode(str(x)).upper())
        
        regiao = cabecalho.loc[cabecalho[0] == "REGIAO:", 1].values[0]
        uf = cabecalho.loc[cabecalho[0] == "UF:", 1].values[0]
        estacao = cabecalho.loc[cabecalho[0] == "ESTACAO:", 1].values[0]
        latitude = cabecalho.loc[cabecalho[0] == "LATITUDE:", 1].values[0]
        longitude = cabecalho.loc[cabecalho[0] == "LONGITUDE:", 1].values[0]
        altitude = cabecalho.loc[cabecalho[0] == "ALTITUDE:", 1].values[0]

        arquivo['Regiao'] = regiao
        arquivo['UF'] = uf
        arquivo['Estacao'] = estacao
        arquivo['Latitude'] = latitude
        arquivo['Longitude'] = longitude
        arquivo['Altitude'] = altitude

        # Salva o arquivo processado como CSV
        output_path = os.path.splitext(arquivo_path)[0] + "_processado.csv"
        arquivo.to_csv(output_path, index=False, encoding='utf-8')
        print(f'Arquivo processado salvo em: {output_path}')
    except Exception as e:
        print(f"Erro ao processar arquivo {arquivo_path}: {e}")

# Caminho base onde estão os arquivos
caminho_base = r'C:\Users\leand\airflow-dbt\arquivos\INMET\INMET'

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='processar_arquivos_inmet',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['processamento', 'dinamico'],
) as dag:

    listar_arquivos = PythonOperator(
        task_id='listar_arquivos',
        python_callable=listar_arquivos_e_pastas,
        op_kwargs={'caminho_base': caminho_base},
    )

    # Grupo de tasks dinâmicas
    def criar_tasks(**kwargs):
        arquivos = kwargs['ti'].xcom_pull(task_ids='listar_arquivos', key='arquivos')
        print(arquivos)
        for arquivo in arquivos:
            task_id = f"processar_{os.path.basename(arquivo).replace('.', '_')}"
            PythonOperator(
                task_id=task_id,
                python_callable=processar_arquivo,
                op_kwargs={'arquivo_path': arquivo},
                dag=dag,
            ).execute(kwargs)
    
    processar_grupo = PythonOperator(
        task_id="criar_tasks",
        python_callable=criar_tasks,
    )

    listar_arquivos >> processar_grupo

