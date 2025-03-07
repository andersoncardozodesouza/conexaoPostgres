import psycopg2
from psycopg2 import OperationalError
from clickhouse_connect import get_client

class Conexao:
    @staticmethod
    def conectar_banco_1():
        try:
            conn = psycopg2.connect(
                dbname="elotech",     # Substitua pelo nome do seu primeiro banco de dados
                user="elotech",     # Substitua pelo usuário do banco 1
                password="elo",   # Substitua pela senha do banco 1
                host="acesso.terraboa.eloweb.net",    # Ou o endereço do servidor PostgreSQL
                port="5432"          # A porta padrão do PostgreSQL
            )
            print("Conexão com Banco 1 bem-sucedida!")
            return conn
        except OperationalError as e:
            print(f"Erro ao conectar ao Banco 1: {e}")
            return None

    @staticmethod
    def conectar_banco_2():
        try:
            conn = psycopg2.connect(
                dbname="jrsistemas",     # Substitua pelo nome do seu segundo banco de dados
                user="jrsistemas",     # Substitua pelo usuário do banco 2
                password="jrm@r1nga",   # Substitua pela senha do banco 2
                host="acesso.terraboa.eloweb.net",    # Ou o endereço do servidor PostgreSQL
                port="5432"          # A porta padrão do PostgreSQL
            )
            print("Conexão com Banco 2 bem-sucedida!")
            return conn
        except OperationalError as e:
            print(f"Erro ao conectar ao Banco 2: {e}")
            return None 

    @staticmethod
    def conectar_banco_3():
        try:
            conn = psycopg2.connect(
                dbname="jrsistemas",     # Substitua pelo nome do seu segundo banco de dados
                user="jrsistemas",     # Substitua pelo usuário do banco 2
                password="jrm@r1nga",   # Substitua pela senha do banco 2
                host="acesso.terraboa.eloweb.net",    # Ou o endereço do servidor PostgreSQL
                port="5432"          # A porta padrão do PostgreSQL
            )
            print("Conexão com Banco 3 bem-sucedida!")
            return conn
        except OperationalError as e:
            print(f"Erro ao conectar ao Banco 3: {e}")
            return None 
        

    @staticmethod
    def testar_conexao(banco):
        """Função que testa a conexão com o banco selecionado (1, 2 ou 3)."""
        if banco == 1:
            return Conexao.conectar_banco_1()
        elif banco == 2:
            return Conexao.conectar_banco_2() 
        elif banco == 3:
            return Conexao.conectar_banco_3() 
        else:
            print("Banco inválido. Escolha entre 1,2,3")
            return None

from clickhouse_connect import get_client