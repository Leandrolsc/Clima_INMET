class PesquisarPorEstadoCidadeETempo:
    def __init__(self, clima_repository):
        self.clima_repository = clima_repository

    def execute(self, uf, cidade, data_inicio, data_fim):
        return self.clima_repository.pesquisar_por_estado_cidade_e_tempo(uf, cidade, data_inicio, data_fim)