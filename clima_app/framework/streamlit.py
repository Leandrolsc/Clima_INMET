import streamlit as st
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from interface_adapters.repositories import ClimaRepository
from use_cases.get_estados import GetEstados
from use_cases.get_cidades_por_estado import GetCidadesPorEstado
from use_cases.pesquisar_por_estado_cidade_e_tempo import PesquisarPorEstadoCidadeETempo

# Caminho do arquivo Parquet
data_source = r'arquivos\clima_inmet_aggregated.parquet'
repo = ClimaRepository(data_source)

# Instanciar use cases
get_estados_use_case = GetEstados(repo)
get_cidades_por_estado_use_case = GetCidadesPorEstado(repo)
pesquisar_por_estado_cidade_e_tempo_use_case = PesquisarPorEstadoCidadeETempo(repo)

# Título da aplicação
st.title('Pesquisa de Clima')

# Seleção de Estado
estado = st.selectbox('Selecione o Estado', get_estados_use_case.execute())

# Seleção de Cidade
cidade = st.selectbox('Selecione a Cidade', get_cidades_por_estado_use_case.execute(estado))

# Seleção de intervalo de tempo
data_inicio = st.date_input('Data de Início', datetime(2023, 1, 1))
data_fim = st.date_input('Data de Fim', datetime(2023, 12, 31))

# Botão de pesquisa
if st.button('Pesquisar'):
    resultados = pesquisar_por_estado_cidade_e_tempo_use_case.execute(estado, cidade, data_inicio, data_fim)
    st.write(resultados)
    # if resultados:
    #     for clima in resultados:
    #         st.write(f"Data: {clima.Data}, Regiao: {clima.Regiao}, UF: {clima.UF}, Estacao: {clima.Estacao}, Max: {clima.Max_Temperatura}, Min: {clima.Min_Temperatura}, Med: {clima.Med_Temperatura}")
    # else:
    #     st.write("Nenhum resultado encontrado para os critérios de pesquisa selecionados.")