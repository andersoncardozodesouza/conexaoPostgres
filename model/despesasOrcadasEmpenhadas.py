class DespesasOrcadasEmpenhadas:
    def __init__(self, dsclassificacaodespesa, valororcado, valorempenhado):
        self.dsclassificacaodespesa = dsclassificacaodespesa
        self.valororcado = valororcado
        self.valorempenhado = valorempenhado

    def to_dict(self):
        return {
            "dsclassificacaodespesa": self.dsclassificacaodespesa,
            "valororcado": self.valororcado,
            "valorempenhado": self.valorempenhado
        }
