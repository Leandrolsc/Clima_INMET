from google.cloud import bigquery

class ClimaRepository:
    def __init__(self, project_id, dataset_id, key_path):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.key_path = key_path

        # Configurar a autenticação com o BigQuery
        self.client = bigquery.Client.from_service_account_json(self.key_path)

    def get_estados(self):
        # Consultar os estados únicos da tabela dEstados_Inmet
        query = f"SELECT DISTINCT sigla_estado FROM `{self.project_id}.{self.dataset_id}.dEstados_Inmet` ORDER BY sigla_estado"
        estados = self.client.query(query).to_dataframe()
        return estados['sigla_estado'].tolist()

    def get_cidades_por_estado(self, estado):
        # Consultar as cidades únicas para o estado especificado na tabela dCidades_Inmet
        query = f"""
        SELECT DISTINCT nome_cidade 
        FROM `{self.project_id}.{self.dataset_id}.dCidades_Inmet` 
        WHERE sigla_estado = @estado
        ORDER BY nome_cidade
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("estado", "STRING", estado)
            ]
        )
        cidades = self.client.query(query, job_config=job_config).to_dataframe()
        return cidades['nome_cidade'].tolist()

    def pesquisar_por_estado_cidade_e_tempo(self, estado, cidade, data_inicio, data_fim):
        # Consultar os dados filtrados por estado, cidade e intervalo de tempo
        query = f"""
        SELECT t.temperatura_maxima,
                t.temperatura_minima,
                t.temperatura_media, 
                c.nome_cidade, 
                e.sigla_estado, 
                cal.Data
        FROM `{self.project_id}.{self.dataset_id}.fTemperatura_Inmet` t
        JOIN `{self.project_id}.{self.dataset_id}.dCidades_Inmet` c ON t.SK_Cidade = c.SK_Cidade
        JOIN `{self.project_id}.{self.dataset_id}.dEstados_Inmet` e ON t.SK_Estado = e.SK_Estado
        JOIN `{self.project_id}.{self.dataset_id}.dCalendario_Inmet` cal ON t.SK_Calendario = cal.SK_Calendario
        WHERE e.sigla_estado = @estado
          AND c.nome_cidade = @cidade
          AND cal.Data BETWEEN @data_inicio AND @data_fim
        ORDER BY cal.Data
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("estado", "STRING", estado),
                bigquery.ScalarQueryParameter("cidade", "STRING", cidade),
                bigquery.ScalarQueryParameter("data_inicio", "DATE", data_inicio),
                bigquery.ScalarQueryParameter("data_fim", "DATE", data_fim),
            ]
        )
        resultados = self.client.query(query, job_config=job_config).to_dataframe()
        return resultados