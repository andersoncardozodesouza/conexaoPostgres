from conexao import Conexao

if __name__ == "__main__":
    banco_teste = 1  # 1, 2 ou 3, dependendo de qual banco você quer testar
    conexao = Conexao.testar_conexao(banco_teste)

    if conexao:
        if banco_teste == 1:
            print(f"Conexão com Banco {banco_teste} - Terra Boa foi bem-sucedida!")
        if banco_teste == 2:
            print(f"Conexão com Banco {banco_teste} - Sarandi foi bem-sucedida!")
        else:
            print(f"Conexão com Banco {banco_teste} - Terra Boa ClickHouse foi bem-sucedida!")
        conexao.close()  # Fechar a conexão após o teste
    else:
        print(f"Falha ao conectar ao Banco {banco_teste}.")
