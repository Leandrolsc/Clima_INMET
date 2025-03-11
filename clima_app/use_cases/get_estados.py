class GetEstados:
    def __init__(self, clima_repository):
        self.clima_repository = clima_repository

    def execute(self):
        return self.clima_repository.get_estados()