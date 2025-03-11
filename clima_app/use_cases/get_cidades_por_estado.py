class GetCidadesPorEstado:
    def __init__(self, clima_repository):
        self.clima_repository = clima_repository

    def execute(self, uf):
        return self.clima_repository.get_cidades_por_estado(uf)