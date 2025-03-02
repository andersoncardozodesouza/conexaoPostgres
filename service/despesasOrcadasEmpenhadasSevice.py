from repository.despesasOrcadasEmpenhadasRepository import DespesasOrcadasEmpenhadasRepository



class DespesasOrcadasEmpenhadasService:
    def __init__(self):
        self.repository_banco_1 = DespesasOrcadasEmpenhadasRepository(1)
        self.repository_banco_2 = DespesasOrcadasEmpenhadasRepository(2)

    def obter_despesasOrcadasEmpenhadasService(self, banco, entidades, idquadrimestre, nrano):
        if banco == 1:
            
            despesas = self.repository_banco_1.obter_despesasOrcadasEmpenhadasRepository(entidades, idquadrimestre, nrano)        
            return [despesa.to_dict() for despesa in despesas]
        
        else:
            raise ValueError("Banco inválido. Escolha entre 1, 2 ou 3.")

    def salvar_despesasOrcadasEmpenhadasService(self, banco, despesas):
        if banco == 2:
            self.repository_banco_2.inserir(despesas)
        else:
            raise ValueError("Banco inválido. Escolha entre 1, 2 ou 3.")