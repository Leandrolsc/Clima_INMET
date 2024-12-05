import os
import pandas as pd
import unidecode
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def convert_temperatura(value):
    if isinstance(value, str):
        # Se for string, substitui a vírgula e converte para float
        return float(value.replace(',', '.'))
    elif isinstance(value, (float, int)):
        # Se for float ou int, retorna o valor como está
        return float(value)
    else:
        # Se for outro tipo (por exemplo, 'N/A'), retorna NaN
        return None


def listar_arquivos_e_pastas(caminho_base):
    arquivos = []
    if not os.path.exists(caminho_base):
        raise FileNotFoundError(f"A pasta {caminho_base} não foi encontrada.")
    for root, dirs, files in os.walk(caminho_base):
        for arquivo in files:
            caminho_arquivo = os.path.join(root, arquivo)
            if os.path.isfile(caminho_arquivo):  # Garante que é um arquivo
                arquivos.append(caminho_arquivo)
    return arquivos


conn_id = "Db"  # Substitua pelo ID da sua conexão
table_name = "Temperatura_historico"  # Nome da tabela no banco
# Obtém a conexão do Airflow
postgres_hook = PostgresHook(postgres_conn_id=conn_id)
engine = postgres_hook.get_sqlalchemy_engine()


def processar_arquivos(arquivos):
    try:
        #df = pd.DataFrame(columns=['Data', 'Temperatura','Regiao','UF','Estacao','Latitude','Longitude','Altitude'])
            print(f'Processando arquivo: {arquivos}')
            arquivo = pd.read_csv(arquivos
                            ,sep=';'
                            ,header=8
                            ,encoding='latin1')
            colunas_arquivo = list(arquivo.columns)
            if 'DATA (YYYY-MM-DD)' in colunas_arquivo:
                colunas = ['DATA (YYYY-MM-DD)','TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)']
                arquivo = arquivo[colunas]
                arquivo.rename(columns={'DATA (YYYY-MM-DD)': 'Data'
                                    ,'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'Temperatura'}
                                    ,inplace=True)
            else:
                colunas = ['Data','TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)']
                arquivo = arquivo[colunas]
                arquivo.rename(columns={'Data': 'Data'
                                    ,'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'Temperatura'}
                                    ,inplace=True)


            arquivo['Temperatura'] = arquivo['Temperatura'].apply(convert_temperatura)
            arquivo = arquivo[arquivo['Temperatura'] != -9999]

            arquivo['Data'] = pd.to_datetime(arquivo['Data'])
            arquivo['Tempo'] = arquivo['Data'].dt.time
            # arquivo['Ano'] = arquivo['Data'].dt.year
            # arquivo['Mes'] = arquivo['Data'].dt.month

            
            cabecalho = pd.read_csv(arquivos
                                    ,delimiter=';'
                                    , nrows=7
                                    , encoding='latin1'
                                    , header=None)
            cabecalho[0] = cabecalho[0].apply(lambda x: unidecode.unidecode(str(x)).upper())
            
            if cabecalho is not None:
                regiao = cabecalho.loc[cabecalho[0] == "REGIAO:", 1].values[0]
                uf = cabecalho.loc[cabecalho[0] == "UF:", 1].values[0]
                estacao = cabecalho.loc[cabecalho[0] == "ESTACAO:", 1].values[0]
                latitude = cabecalho.loc[cabecalho[0] == "LATITUDE:", 1].values[0]
                longitude = cabecalho.loc[cabecalho[0] == "LONGITUDE:", 1].values[0]
                altitude = cabecalho.loc[cabecalho[0] == "ALTITUDE:", 1].values[0]
            else:
                print(f'Erro ao criar dataset {arquivos}')        

            arquivo['Regiao'] = regiao
            arquivo['UF'] = uf
            arquivo['Estacao'] = estacao
            arquivo['Latitude'] = latitude
            arquivo['Longitude'] = longitude
            arquivo['Altitude'] = altitude
            
            # Inserção no banco de dados
            arquivo.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Arquivo {arquivos} processado e inserido com sucesso.")
    except Exception as e:
        print(f"Erro ao processar o arquivo {arquivos}: {e}")

caminho_base = '/opt/airflow/arquivos/INMET/INMET'  # Substitua pelo caminho correto                   

def processamento_arquivos():
    arquivos = listar_arquivos_e_pastas(caminho_base)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processar_arquivos, arquivos)

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='dag_processamento_CSV',
    default_args=default_args,
    description='Insere dados do arquivo csv no PostgreSQL usando Connection',
    schedule_interval=None,  # Execução manual
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['postgres', 'csv', 'extracao']
) as dag:
    inserir_dados_task = PythonOperator(
        task_id='inserir_dados',
        python_callable=processamento_arquivos
    )

    inserir_dados_task
