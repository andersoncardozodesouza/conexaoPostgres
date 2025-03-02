from conexao import Conexao
from model.despesasOrcadasEmpenhadas import DespesasOrcadasEmpenhadas
import psycopg2

class DespesasOrcadasEmpenhadasRepository:
    
    def __init__(self, banco):
        if banco == 1:
            self.conn = Conexao.conectar_banco_1()
        elif banco == 2:
            self.conn = Conexao.conectar_banco_2()
        elif banco == 3:
            self.conn = Conexao.conectar_banco_3()
        else:
            raise ValueError("Banco inválido. Escolha entre 1, 2 ou 3.")

    def obter_despesasOrcadasEmpenhadasRepository(self, idtce, idquadrimestre, nrano):

    # Join the values in each list separately
        idtces = ','.join([str(x) for x in idtce])
        idquadrimestres = ','.join([str(x) for x in idquadrimestre])
        nranos = ','.join([str(x) for x in nrano])
        
        query = """
                select  row_number ()over (partition by cdEntidade, idtce, nrano )
                        cdEntidade,
                        idtce,
                        nrano,
                        idquadrimestre, 
                        dsquadrimestre,
                        idclassificacaodespesa,
                        dsclassificacaodespesa, 
                        sum(valorOrcado) as valorOrcado,
                        sum(valorRealizado) as valorRealizado,
                        case when nrano <= 2024 then true
                        else false end as status
                    from(
                    select cdEntidade::bigint                            as cdEntidade,
                        e.identificacaotce::bigint                    as idTce,
                        nrAno::bigint                                 as nrAno,
                        idQuadrimestre::bigint                        as idQuadrimestre,
                        dsQuadrimestre::text                          as dsQuadrimestre,
                        substring(idClassificacaoDespesa,1,2)::text   as idClassificacaoDespesa,
                        substring(idClassificacaoDespesa,1,2)||' - '||dsClassificacaoDespesa::text         as dsClassificacaoDespesa,
                        valorOrcado::numeric(16,4)                    as valorOrcado,
                        valorRealizado::numeric(16,4)                 as valorRealizado,
                        status::text                                  as status
                    from(
                    select cdEntidade               as cdEntidade,
                        nrAno                    as nrAno,
                        case when extract(month from data) between 1 and 4 then 1
                                when extract(month from data) between 5 and 8 then 2
                        else 3 end                             as idQuadrimestre,
                        case when extract(month from data) between 1 and 4 then '1° Quadrimestre'
                                when extract(month from data) between 5 and 8 then '2° Quadrimestre'
                        else '3° Quadrimestre' end               as dsQuadrimestre,
                        data                     as data,
                        reduzido                 as reduzido,
                        idClassificacaoDespesa   as idClassificacaoDespesa,
                        dsClassificacaoDespesa   as dsClassificacaoDespesa,
                        idDespesa                as idDespesa,
                        dsDespesa                as dsDespesa,
                        cdFonte                  as cdFonte,
                        valorOrcado              as valorOrcado,
                        valorRealizado           as valorRealizado,
                        status                   as status
                    from(
                    select cdEntidade,
                        nrAno,
                        data,
                        reduzido,
                        idClassificacaoDespesa,
                        dd.descricao  as dsClassificacaoDespesa,
                        idDespesa,
                        dsDespesa,
                        cdFonte,
                        valorOrcado,
                        valorRealizado,
                        status
                        from( 
                    select d.entidade  as cdEntidade, 
                        d.exercicio  as nrAno, 
                        (d.exercicio||'-01-01')::date as data,
                        d.reduzido as reduzido,
                        substring(d.programatica,19,2)||'0000' as idClassificacaoDespesa,
                        '' as dsClassificacaoDespesa,
                        substring(d.programatica,19,6) as idDespesa, 
                        descricao as dsDespesa,
                        fonterecurso as cdFonte,
                        d.valorprevisto  as valorOrcado,
                        0 as valorRealizado,
                        'Orcado'::text as status
                    from siscop.despesa d 
                        where movsn = 'S')despesaOrcada
                    inner join siscop.desdobradesp dd 
                            on dd.entidade = despesaOrcada.cdEntidade
                            and dd.exercicio = despesaOrcada.nrAno
                            and dd.despesa = despesaOrcada.idClassificacaoDespesa)despesaOrcada
                    union all
                    select cdEntidade               as cdEntidade,
                        nrAno                    as nrAno,
                        case when extract(month from data) between 1 and 4 then 1
                                when extract(month from data) between 5 and 8 then 2
                        else 3 end                             as idQuadrimestre,
                        case when extract(month from data) between 1 and 4 then '1° Quadrimestre'
                                when extract(month from data) between 5 and 8 then '2° Quadrimestre'
                        else '3° Quadrimestre' end               as dsQuadrimestre,
                        data                     as data,
                        reduzido                 as reduzido,
                        idClassificacaoDespesa   as idClassificacaoDespesa,
                        dsClassificacaoDespesa   as dsClassificacaoDespesa,
                        idDespesa                as idDespesa,
                        dsDespesa                as dsDespesa,
                        cdFonte                  as cdFonte,
                        valorOrcado              as valorOrcado,
                        valorRealizado           as valorRealizado,
                        status  
                    from(
                    select cdEntidade,
                        nrAno,
                        data,
                        reduzido,
                        idClassificacaoDespesa,
                        dd.descricao  as dsClassificacaoDespesa,
                        idDespesa,
                        dsDespesa,
                        cdFonte,
                        valorOrcado,
                        valorRealizado,
                        status
                    from(   
                    select entidade as cdEntidade,
                        exercicio as nrAno,
                        data::date as data,
                        0 as reduzido,
                        substring(e.programatica,19,2)||'0000' as idClassificacaoDespesa,
                        '' as dsClassificacaoDespesa,
                        e.despesa as idDespesa,
                        descricaodespesa as dsDespesa,
                        fonterecurso as cdFonte,
                        0 as valorOrcado,
                        e.valor as valorRealizado,
                        'Empenhado'::text as status
                        from siscop.empenho e )despesaEmpenhada
                    inner join siscop.desdobradesp dd 
                            on dd.entidade = despesaEmpenhada.cdEntidade
                            and dd.exercicio = despesaEmpenhada.nrAno
                            and dd.despesa = despesaEmpenhada.idClassificacaoDespesa)despesaEmpenhada
                    union all
                    select cdEntidade               as cdEntidade,
                        nrAno                    as nrAno,
                        case when extract(month from data) between 1 and 4 then 1
                                when extract(month from data) between 5 and 8 then 2
                        else 3 end                             as idQuadrimestre,
                        case when extract(month from data) between 1 and 4 then '1° Quadrimestre'
                                when extract(month from data) between 5 and 8 then '2° Quadrimestre'
                        else '3° Quadrimestre' end               as dsQuadrimestre,
                        data                     as data,
                        reduzido                 as reduzido,
                        idClassificacaoDespesa   as idClassificacaoDespesa,
                        dsClassificacaoDespesa   as dsClassificacaoDespesa,
                        idDespesa                as idDespesa,
                        dsDespesa                as dsDespesa,
                        cdFonte                  as cdFonte,
                        valorOrcado              as valorOrcado,
                        valorRealizado           as valorRealizado,
                        status  
                    from(
                    select cdEntidade,
                        nrAno,
                        data,
                        reduzido,
                        idClassificacaoDespesa,
                        dd.descricao  as dsClassificacaoDespesa,
                        idDespesa,
                        dsDespesa,
                        cdFonte,
                        valorOrcado,
                        valorRealizado*-1 as valorRealizado,
                        status
                    from(   
                    select e.entidade as cdEntidade,
                        e.exercicio as nrAno,
                        ae.data::date as data,
                        0 as reduzido,
                        substring(e.programatica,19,2)||'0000' as idClassificacaoDespesa,
                        '' as dsClassificacaoDespesa,
                        e.despesa as idDespesa,
                        descricaodespesa as dsDespesa,
                        fonterecurso as cdFonte,
                        0 as valorOrcado,
                        ae.valor as valorRealizado,
                        'AnulacaoEmpenhado'::text as status
                        from siscop.empenho e
                    inner join siscop.anulacaoempenho ae 
                            on ae.entidade = e.entidade
                            and ae.exercicio = e.exercicio
                            and ae.empenho = e.empenho
                            and ae.unidadeorcamentaria = e.unidadeorcamentaria)despesaEmpenhada
                    inner join siscop.desdobradesp dd 
                            on dd.entidade = despesaEmpenhada.cdEntidade
                            and dd.exercicio = despesaEmpenhada.nrAno
                            and dd.despesa = despesaEmpenhada.idClassificacaoDespesa)despesaAnulacaoEmpenhada         
                    union all         
                    select cdEntidade               as cdEntidade,
                        nrAno                    as nrAno,
                        case when extract(month from data) between 1 and 4 then 1
                                when extract(month from data) between 5 and 8 then 2
                        else 3 end                             as idQuadrimestre,
                        case when extract(month from data) between 1 and 4 then '1° Quadrimestre'
                                when extract(month from data) between 5 and 8 then '2° Quadrimestre'
                        else '3° Quadrimestre' end               as dsQuadrimestre,
                        data                     as data,
                        reduzido                 as reduzido,
                        idClassificacaoDespesa   as idClassificacaoDespesa,
                        dsClassificacaoDespesa   as dsClassificacaoDespesa,
                        idDespesa                as idDespesa,
                        dsDespesa                as dsDespesa,
                        cdFonte                  as cdFonte,
                        valorOrcado              as valorOrcado,
                        valorRealizado           as valorRealizado,
                        status  
                    from(
                    select cdEntidade,
                        nrAno,
                        data,
                        reduzido,
                        idClassificacaoDespesa,
                        dd.descricao  as dsClassificacaoDespesa,
                        idDespesa,
                        dsDespesa,
                        cdFonte,
                        valorOrcado,
                        valorRealizado,
                        status
                    from(   
                    select e.entidade as cdEntidade,
                        e.exercicio as nrAno,
                        ae.data::date as data,
                        0 as reduzido,
                        substring(e.programatica,19,2)||'0000' as idClassificacaoDespesa,
                        '' as dsClassificacaoDespesa,
                        e.despesa as idDespesa,
                        descricaodespesa as dsDespesa,
                        fonterecurso as cdFonte,
                        0 as valorOrcado,
                        ae.valor as valorRealizado,
                        'EstornoAnulacaoEmpenhado'::text as status
                        from siscop.empenho e
                    inner join siscop.anulacaoempenho ae 
                            on ae.entidade = e.entidade
                            and ae.exercicio = e.exercicio
                            and ae.empenho = e.empenho
                            and ae.unidadeorcamentaria = e.unidadeorcamentaria 
                            and ae.dataestorno is not null)despesaEmpenhada
                    inner join siscop.desdobradesp dd 
                            on dd.entidade = despesaEmpenhada.cdEntidade
                            and dd.exercicio = despesaEmpenhada.nrAno
                            and dd.despesa = despesaEmpenhada.idClassificacaoDespesa)despesaEstornoAnulacaoEmpenhada)despesaOrcadaEmpenhada
                    inner join siscop.entidade e 
                            on e.entidade = despesaOrcadaEmpenhada.cdentidade
                    where nrAno between date_part('year', current_date) - 4 and date_part('year', current_date))despesaOrcadaEmpenhada 
                        where idtce = {}
                          and idquadrimestre in ({})
                          and nrano = {}
                    group by cdEntidade,idtce,nrano,idquadrimestre, dsquadrimestre,idclassificacaodespesa,
                            dsclassificacaodespesa
                    order by idtce, nrano, dsquadrimestre, dsclassificacaodespesa
        """.format(idtces, idquadrimestres, nranos)
        

        # Debugging the generated query
        print("Generated SQL query: ", query)

        # Execute the query
        client = self.conexao.obter_cliente()
        resultado = client.query(query).result_rows
        
        # Process the results
        despesas = [DespesasOrcadasEmpenhadas(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]) for r in resultado]

        return despesas

    def inserir(self, despesas):
        cursor = self.conn.cursor()
        for despesa in despesas:
            query = """
            INSERT INTO aud.auddespesasOrcadasEmpenhadas (id, cdentidade, idtce, nrano, idquadrimestre,
                                                          dsquadrimestre,idclassificacaodespesa,
                                                          dsclassificacaodespesa,valororcado, valorrealizado, 
                                                          status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(query, (despesa.id, despesa.cdentidade, despesa.idtce, despesa.nrano, despesa.idquadrimestre,
                                   despesa.dsquadrimestre, despesa.idclassificacaodespesa, despesa.dsclassificacaodespesa,
                                    despesa.valororcado, despesa.valorrealizado, despesa.statu ))

        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()