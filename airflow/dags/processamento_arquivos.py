import os
import pandas as pd
import unidecode

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
    # Verifica se o caminho existe
    if not os.path.exists(caminho_base):
        print(f"A pasta {caminho_base} não foi encontrada.")
        return
    
    # Itera sobre o conteúdo da pasta base
    for root, dirs, files in os.walk(caminho_base):
        # Lista todas as subpastas
        for subpasta in dirs:
            #print(f"Pasta encontrada: {subpasta}")
            
            # Caminho completo da subpasta
            caminho_subpasta = os.path.join(root, subpasta)
            
            # Listando os arquivos dentro da subpasta
            for arquivo in os.listdir(caminho_subpasta):
                caminho_arquivo = os.path.join(caminho_subpasta, arquivo)
                if os.path.isfile(caminho_arquivo):
                    arquivos.append(caminho_arquivo) 
    return arquivos


caminho_base = r'C:\Users\leand\airflow-dbt\arquivos\INMET\INMET'  # Substitua pelo caminho correto                   
# Chamando a funçãO: 


arquivos = listar_arquivos_e_pastas(caminho_base)

def processar_arquivos(arwe):
    df = pd.DataFrame(columns=['Data', 'Temperatura','Regiao','UF','Estacao','Latitude','Longitude','Altitude'])
    for rows in arquivos:
        print(f'Processando arquivo: {rows}')
        arquivo = pd.read_csv(rows
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
        #arquivo['Ano'] = arquivo['Data'].dt.year
        #arquivo['Mes'] = arquivo['Data'].dt.month

        
        cabecalho = pd.read_csv(rows
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
            print(f'Erro ao criar dataset {rows}')        

        arquivo['Regiao'] = regiao
        arquivo['UF'] = uf
        arquivo['Estacao'] = estacao
        arquivo['Latitude'] = latitude
        arquivo['Longitude'] = longitude
        arquivo['Altitude'] = altitude
        
        # Agrupando por 'Ano' e 'Estacao' e calculando o mínimo, médio e máximo da coluna 'Temperatura'
        #media_por_ano = arquivo.groupby(['Ano', 'Estacao'])['Temperatura'].agg(['min', 'mean', 'max'])
        # Resetando o índice para transformar em um DataFrame normal
        df1 = arquivo
        # Renomeando as colunas
        df1.columns = ['Data', 'Temperatura','Regiao','UF','Estacao','Latitude','Longitude','Altitude']

        df = pd.concat([df, df1], ignore_index=True)
        print(f'Arquivo: {rows} foi processado com sucesso')
    return df


#df_final = processar_arquivos()


from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=3) as executor:
    resultados = list(executor.map(processar_arquivos, arquivos))