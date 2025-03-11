from entities.clima import Clima
import pandas as pd

class ClimaRepository:
    def __init__(self, data_source):
        self.data_source = data_source
        self.df = pd.read_parquet(data_source)
        # Converter a coluna 'Data' para o tipo datetime.date
        self.df['Data'] = pd.to_datetime(self.df['Data']).dt.date

    def load_clima(self):
        return self.df

    def get_estados(self):
        return self.df['UF'].unique()

    def get_cidades_por_estado(self, uf):
        return self.df[self.df['UF'] == uf]['Cidade'].unique()

    def pesquisar_por_estado_cidade_e_tempo(self, uf, cidade, data_inicio, data_fim):
        # Converter data_inicio e data_fim para datetime.date
        data_inicio = pd.to_datetime(data_inicio).date()
        data_fim = pd.to_datetime(data_fim).date()
        
        resultados = self.df[(self.df['UF'] == uf) & (self.df['Cidade'] == cidade) & (self.df['Data'] >= data_inicio) & (self.df['Data'] <= data_fim)]
        #climas = [Clima(row['Data'], row['Regiao'], row['UF'], row['Estacao'], row['Latitude'], row['Longitude'], row['Max_Temperatura'], row['Min_Temperatura'], row['Med_Temperatura']) for index, row in resultados.iterrows()]
        return resultados