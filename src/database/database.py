import psycopg2
from urllib.parse import urlparse
import logging
import sys
import pathlib

path_src = str(pathlib.Path(__file__).parent.resolve()) + '/../'
sys.path.append(path_src)

from configuration.config import ConfigurationProject

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        POSTGRES_CONNECTION_STRING = 'localhost://monkey:monkey@monkey_business:5432'
        p = urlparse(POSTGRES_CONNECTION_STRING)

        pg_connection = {
            'dbname': p.hostname,
            'user': p.username,
            'password': p.password,
            'port': p.port,
            'host': p.scheme
        }

        con = psycopg2.connect(**pg_connection)
        con.autocommit = True
        self.cursor = con.cursor()
        # self.cria_tabela_empresas()
        # self.dados_brutos()

    def cria_tabela_empresas(self):
        try:
            sql = f"""
                        CREATE TABLE IF NOT EXISTS public.empresa 
                            (
                                id serial4 NOT NULL,
                                codigo text NULL,
                                descricao varchar(500) NULL,
                                cnpj text NULL,
                                data_situacao text NULL,
                                motivo_situacao text NULL,
                                tipo text NULL,
                                nome text NULL,
                                fantasia text NULL,
                                porte text NULL,
                                natureza_juridica text NULL,
                                abertura text NULL,
                                email text NULL,
                                qsa text NULL,
                                situacao text NULL,
                                logradouro text NULL,
                                numero text NULL,
                                municipio text NULL,
                                bairro text NULL,
                                uf text NULL,
                                telefone text NULL,
                                cep text NULL,
                                complemento text NULL,
                                efr text NULL,
                                situacao_especial text NULL,
                                data_situacao_especial text NULL,
                                atividade_principal text NULL,
                                capital_social text NULL,
                                extra text NULL,
                                billing text NULL,
                                ultima_atualizacao text NULL,
                                status text NULL,
                                created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
                            );
                    """
            self.cursor.execute(sql)
            logger.info('tabela empresa - OK')            
        except Exception as e:
            logger.info(str(e))
    
    def insere_dados_empresa(self, data, cnpj):
        try:
            sql = f"""
                BEGIN;
                LOCK TABLE public.empresa IN SHARE ROW EXCLUSIVE MODE;
                INSERT INTO public.empresa
                    (
                        codigo,
                        descricao,
                        cnpj,
                        data_situacao,
                        motivo_situacao,
                        tipo,
                        nome,
                        fantasia,
                        porte,
                        natureza_juridica,
                        abertura,
                        email,
                        qsa,
                        situacao,
                        logradouro,
                        numero,
                        municipio,
                        bairro,
                        uf,
                        telefone,
                        cep,
                        complemento,
                        efr,
                        situacao_especial,
                        data_situacao_especial,
                        atividade_principal,
                        capital_social,
                        extra,
                        billing,
                        ultima_atualizacao,
                        status
                    )
                VALUES({data});
            """
            print('aqui')
            print(data)
            try:
                self.cursor.execute(sql)
                print('feito')
            except Exception as e:
                print(str(e))
            logger.info(f'dados cnpj {cnpj} inseridos') 
        except Exception as e:
            logger.info(str(e))
    
    def dados_brutos(self):
        try:
            sql = f"""
                CREATE TABLE IF NOT EXISTS public.cadastro_bruto 
                    (   
                        id serial4 NOT NULL,
                        cnpj varchar(100) NULL,
                        conteudo text,
                        created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT nfe_pkey PRIMARY KEY (id)
                    );
            """
            self.cursor.execute(sql)
            logger.info('tabela cadastro_bruto - OK') 
        except Exception as e:
            logger.info(str(e))
    
    def insere_dados_brutos(self, cnpj, data):
        try:
            sql = f"""
            BEGIN;
            LOCK TABLE public.cadastro_bruto IN SHARE ROW EXCLUSIVE MODE;
            INSERT INTO public.cadastro_bruto
                (
                    cnpj, 
                    conteudo
                )
            SELECT cnpj, '{data[1].replace("'",'').replace('%', '')}'
            
            COMMIT;
            """
            self.cursor.execute(sql, data)
        except Exception as e:
            logger.info(str(e))
    
    def cria_tabelas_tmp(self):
        try:
            logger.info('inicio criacao tabelas temporarias ....')
            try:
                sql_socio_pj = f"""
                CREATE TABLE IF NOT EXISTS PUBLIC.tmp_socio_pj AS
                (
                    SELECT  a.st_cnpj_base          AS cnpj_base,
                            a.st_nome               AS nome,
                            a.st_cpf_cnpj           AS cpf_cnpj,
                            b.st_qualificacao       AS qualificacao_socio,
                            a.dt_entrada            AS data_entrada,
                            d.st_pais               AS pais,
                            a.st_nome_representante AS nome_representante,
                            c.st_qualificacao       AS qualificacao_representante
                    FROM       PUBLIC.tb_socio a
                    INNER JOIN PUBLIC.tb_qualificacao_socio b
                        ON         b.cd_qualificacao = a.cd_qualificacao
                    INNER JOIN PUBLIC.tb_qualificacao_socio c
                        ON         c.cd_qualificacao = a.cd_qualificacao
                    INNER JOIN PUBLIC.tb_pais d
                        ON         d.cd_pais = a.cd_pais
                    WHERE      a.cd_tipo = '1'
                );
                """
                self.cursor.execute(sql_socio_pj)
                logger.info('tabela public.tmp_socio_pj criada')
            except Exception as e:
                logger.info(str(e))
            
            try:
                sql_socio_estrangeiro = f"""
                CREATE TABLE IF NOT EXISTS PUBLIC.tmp_socio_estrangeiro AS
                (
                    SELECT  a.st_cnpj_base          AS cnpj_base,
                            a.st_nome               AS nome,
                            a.st_cpf_cnpj           AS cpf_cnpj,
                            b.st_qualificacao       AS qualificacao_socio,
                            a.dt_entrada            AS data_entrada,
                            d.st_pais               AS pais,
                            a.st_nome_representante AS nome_representante,
                            c.st_qualificacao       AS qualificacao_representante
                    FROM       PUBLIC.tb_socio a
                    INNER JOIN PUBLIC.tb_qualificacao_socio b
                        ON         b.cd_qualificacao = a.cd_qualificacao
                    INNER JOIN PUBLIC.tb_qualificacao_socio c
                        ON         c.cd_qualificacao = a.cd_qualificacao
                    INNER JOIN PUBLIC.tb_pais d
                        ON         d.cd_pais = a.cd_pais
                    WHERE      a.cd_tipo = '3'
                );
                """
                self.cursor.execute(sql_socio_estrangeiro)
                logger.info('tabela public.tmp_socio_estrangeiro criada')
            except Exception as e:
                logger.info(str(e))

            try:
                sql_matriz_filiais = f"""
                CREATE TABLE IF NOT EXISTS PUBLIC.tmp_matriz_filiais AS
                (
                    SELECT  st_cnpj_base			AS cnpj_base,
                            COUNT(st_cnpj_base)	    AS quantidade_empresa
                    FROM   PUBLIC.tb_estabelecimento
                    GROUP  BY st_cnpj_base
                );
                """
                self.cursor.execute(sql_matriz_filiais)
                logger.info('tabela public.tmp_matriz_filiais criada')
            except Exception as e:
                logger.info(str(e))
            
            try:
                sql_numero_socios = f"""
                CREATE TABLE IF NOT EXISTS PUBLIC.tmp_numero_socios AS
                (
                    SELECT CONCAT(B.st_cnpj_base, B.st_cnpj_ordem, B.st_cnpj_dv) AS cnpj,
                           COUNT(1)                                              AS qtd_contato
                    FROM   PUBLIC.tb_socio A
                    LEFT JOIN PUBLIC.tb_estabelecimento B
                        ON A.st_cnpj_base = B.st_cnpj_base
                    WHERE  ( B.st_email IS NOT NULL OR B.st_ddd1 IS NOT NULL )
                    GROUP  BY Concat(B.st_cnpj_base, B.st_cnpj_ordem, B.st_cnpj_dv)
                );
                """
                self.cursor.execute(sql_numero_socios)
                logger.info('tabela public.tmp_numero_socios criada')
            except Exception as e:
                logger.info(str(e))

            try:
                sql_socio_adm = f"""
                CREATE TABLE IF NOT EXISTS PUBLIC.tmp_socio_adm as
                (
                    SELECT  B.st_cnpj_base                                                 AS cnpj_base,
                            B.st_cnpj_ordem                                                AS cnpj_ordem,
                            CONCAT(B.st_cnpj_base, B.st_cnpj_ordem, B.st_cnpj_dv)          AS cnpj,
                            CASE
                                WHEN B.cd_matriz_filial = '1' THEN 'Matriz'
                                ELSE 'Filial'
                            END                                                            AS matriz_filial,
                            CAST(REPLACE(A.vl_capital_social, ',', '.') AS DECIMAL(15, 2)) AS capital_social,
                            A.st_razao_social                                              AS razao_social,
                            B.st_nome_fantasia                                             AS nome_fantasia,
                            G.st_natureza_juridica                                         AS natureza_juridica,
                            B.cd_situacao_cadastral                                        AS codigo_situacao_cadastral,
                            B.dt_situacao_cadastral                                        AS data_situacao_cadastral,
                            F.st_motivo_situacao_cadastral                                 AS motivo_situacao_cadastral,
                            B.st_cidade_exterior                                           AS cidade_exterior,
                            E.st_pais                                                      AS pais_exterior,
                            B.dt_inicio_atividade                                          AS data_inicio_atividade,
                            B.cd_cnae_principal                                            AS cnae_principal,
                            C.st_cnae                                                      AS descricao_cnae,
                            B.cd_cnae_secundario                                           AS cnae_secundario,
                            B.st_tipo_logradouro                                           AS tipo_logradouro,
                            B.st_logradouro                                                AS logradouro,
                            B.st_numero                                                    AS numero,
                            TRIM(B.st_complemento)                                         AS complemento,
                            B.st_bairro                                                    AS bairro,
                            B.st_cep                                                       AS cep,
                            B.st_uf                                                        AS uf,
                            D.st_municipio                                                 AS municipio,
                            CASE
                                WHEN B.st_uf IN (   'AC', 'AL', 'AP', 'AM',
                                                    'BA', 'CE', 'DF', 'ES',
                                                    'GO', 'MA', 'MT', 'MS',
                                                    'MG', 'PA', 'PB', 'PR',
                                                    'PE', 'PI', 'RJ', 'RN',
                                                    'RS', 'RO', 'RR', 'SC',
                                                    'SP', 'SE', 'TO' ) THEN 'Brasil'
                                ELSE ''
                            END                                                            AS pais,
                            B.st_ddd1                                                      AS ddd1,
                            B.st_telefone1                                                 AS telefone1,
                            B.st_ddd2                                                      AS ddd2,
                            B.st_telefone2                                                 AS telefone2,
                            B.st_ddd_fax                                                   AS ddd_fax,
                            B.st_fax                                                       AS fax,
                            LOWER(B.st_email)                                              AS email,
                            B.st_situacao_especial                                         AS situacao_especial,
                            B.dt_situacao_especial                                         AS data_situacao_especial
                    FROM   PUBLIC.tb_empresa A
                    INNER JOIN PUBLIC.tb_estabelecimento B
                        ON A.st_cnpj_base = B.st_cnpj_base
                    LEFT JOIN PUBLIC.tb_cnae C
                        ON B.cd_cnae_principal = C.cd_cnae
                    LEFT JOIN PUBLIC.tb_municipios D
                        ON D.cd_municipio = B.cd_municipio
                    LEFT JOIN PUBLIC.tb_pais E
                        ON E.cd_pais = B.cd_pais
                    LEFT JOIN PUBLIC.tb_motivo_situacao_cadastral F
                        ON F.cd_motivo_situacao_cadastral = B.cd_motivo_situacao_cadastral
                    LEFT JOIN PUBLIC.tb_natureza_juridica G
                        ON A.cd_natureza_juridica = G.cd_natureza_juridica
                    LEFT JOIN PUBLIC.tb_dados_simples H
                        ON H.st_cnpj_base = A.st_cnpj_base
            );	
            """
                self.cursor.execute(sql_socio_adm)
                logger.info('tabela public.tmp_socio_adm criada')
            except Exception as e:
                logger.info(str(e))
            
            logger.info('.... fim criacao tabelas temporarias')
        
        except Exception as e:
            logger.info(str(e))
    
    def cria_tabela_fato(self):
        self.cria_tabelas_tmp()

        try:
            logger.info('inicio criacao fato ...')
            sql_cria_fato = f"""
            CREATE TABLE IF NOT EXISTS public.fat_dados_empresa AS
            (
                SELECT  A.cnpj_base,
                        A.cnpj_ordem,
                        A.cnpj,
                        A.matriz_filial,
                        D.quantidade_empresa,
                        E.qtd_contato                   AS contatos_validos,
                        A.capital_social,
                        A.razao_social,
                        A.nome_fantasia,
                        A.natureza_juridica,
                        A.codigo_situacao_cadastral,
                        A.data_situacao_cadastral,
                        A.motivo_situacao_cadastral,
                        A.cidade_exterior,
                        A.pais_exterior,
                        A.data_inicio_atividade,
                        A.cnae_principal,
                        A.descricao_cnae,
                        A.cnae_secundario,
                        A.tipo_logradouro,
                        A.logradouro,
                        A.numero,
                        A.complemento,
                        A.bairro,
                        A.cep,
                        A.uf,
                        A.municipio,
                        A.pais,
                        A.ddd1,
                        A.telefone1,
                        A.ddd2,
                        A.telefone2,
                        A.ddd_fax,
                        A.fax,
                        A.email,
                        A.situacao_especial,
                        A.data_situacao_especial,
                        B.nome                          AS empresa_socia,
                        B.cpf_cnpj                      AS cnpj_empresa_socia,
                        B.qualificacao_socio            AS qualificacao_empresa_socia,
                        B.pais                          AS pais_empresa_socia,
                        B.nome_representante            AS representante_empresa_socia,
                        C.nome                          AS socio_estrangeiro,
                        C.qualificacao_socio            AS qualificacao_socio_estrangeiro,
                        C.pais                          AS pais_socio_estrangeiro,
                        C.nome_representante            AS representante_socio_estrangeiro,
                        NOW()                           AS created_at
                    FROM   PUBLIC.tmp_socio_adm A
                    LEFT JOIN PUBLIC.tmp_socio_pj B
                        ON A.cnpj_base = B.cnpj_base
                    LEFT JOIN PUBLIC.tmp_socio_estrangeiro C
                        ON A.cnpj_base = C.cnpj_base
                    LEFT JOIN PUBLIC.tmp_matriz_filiais D
                        ON A.cnpj_base = D.cnpj_base
                    LEFT JOIN PUBLIC.tmp_numero_socios E
                        ON A.cnpj = E.cnpj 
            ); 
            """
            self.cursor.execute(sql_cria_fato)
            logger.info('... fim criacao tabela fato')
        except Exception as e:
            logger.info(str(e))
        
        try:
            logger.info('dropa temporarias')
            sql_drop_tmp = f"""
                DROP TABLE PUBLIC.tmp_socio_pj;
                DROP TABLE PUBLIC.tmp_socio_estrangeiro;
                DROP TABLE PUBLIC.tmp_matriz_filiais;
                DROP TABLE PUBLIC.tmp_socio_adm;
                DROP TABLE PUBLIC.tmp_numero_socios;
            """
            self.cursor.execute(sql_drop_tmp)
            logger.info('tabelas temporarias dropadas')
        except Exception as e:
            logger.info(str(e))
