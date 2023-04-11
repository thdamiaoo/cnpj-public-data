import psycopg2
from urllib.parse import urlparse
import logging
import sys
import pathlib
import pandas as pd

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

        self.con = psycopg2.connect(**pg_connection)
        self.con.autocommit = True
        self.cursor = self.con.cursor()
        # self.cria_tabela_empresas()
        # self.dados_brutos()
    
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
                sql_socio_adm = f"""
                CREATE TABLE IF NOT EXISTS public.tmp_socio_adm AS
                (
                    SELECT  a.st_cnpj_base    AS cnpj_base,
                            a.st_nome         AS nome_socio_adm,
                            a.st_cpf_cnpj     AS cpf_cnpj,
                            b.st_qualificacao AS qualificacao_socio,
                            a.dt_entrada      AS data_entrada,
                            d.st_pais         AS pais
                    FROM   public.tb_socio a
                    INNER JOIN public.tb_qualificacao_socio b
                        ON b.cd_qualificacao = a.cd_qualificacao
                    INNER JOIN public.tb_pais d
                        ON d.cd_pais = a.cd_pais
                    WHERE  a.cd_tipo = '2'
                        AND b.cd_qualificacao = '49');
                """
                self.cursor.execute(sql_socio_adm)
                logger.info('tabela public.tmp_socio_adm criada')
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
                sql_empresa_geral = f"""
                CREATE TABLE IF NOT EXISTS PUBLIC.tmp_empresa_geral as
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
                            B.dt_situacao_especial                                         AS data_situacao_especial,
                            H.st_opcao_simples                                             AS opcao_simples,
                            H.dt_opcao_simples                                             AS data_opcao_simples,
                            H.dt_exclusao_simples                                          AS data_exclusao_simples,
                            H.st_opcao_mei                                                 AS opcao_mei,
                            H.dt_opcao_mei                                                 AS data_opcao_mei,
                            H.dt_exclusao_mei                                              AS data_exclusao_mei
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
                self.cursor.execute(sql_empresa_geral)
                logger.info('tabela public.tmp_empresa_geral criada')
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
                SELECT  row_number() OVER (PARTITION by 0) AS sk_dados_empresa,
                        A.cnpj_base,
                        A.cnpj_ordem,
                        A.cnpj,
                        A.matriz_filial,
                        D.quantidade_empresa,
                        E.qtd_contato                       AS contatos_validos,
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
                        A.opcao_simples,
                        A.data_opcao_simples,
                        A.data_exclusao_simples,
                        A.opcao_mei,
                        A.data_opcao_mei,
                        A.data_exclusao_mei,
                        F.nome_socio_adm                    AS socio_adm,
                        F.pais                              AS pais_socio_adm,
                        B.nome                              AS empresa_socia,
                        B.cpf_cnpj                          AS cnpj_empresa_socia,
                        B.qualificacao_socio                AS qualificacao_empresa_socia,
                        B.pais                              AS pais_empresa_socia,
                        B.nome_representante                AS representante_empresa_socia,
                        C.nome                              AS socio_estrangeiro,
                        C.qualificacao_socio                AS qualificacao_socio_estrangeiro,
                        C.pais                              AS pais_socio_estrangeiro,
                        C.nome_representante                AS representante_socio_estrangeiro,
                        NOW()                               AS created_at
                    FROM   PUBLIC.tmp_empresa_geral A
                    LEFT JOIN PUBLIC.tmp_socio_pj B
                        ON A.cnpj_base = B.cnpj_base
                    LEFT JOIN PUBLIC.tmp_socio_estrangeiro C
                        ON A.cnpj_base = C.cnpj_base
                    LEFT JOIN PUBLIC.tmp_matriz_filiais D
                        ON A.cnpj_base = D.cnpj_base
                    LEFT JOIN PUBLIC.tmp_numero_socios E
                        ON A.cnpj = E.cnpj
                    LEFT JOIN PUBLIC.tmp_socio_adm F
                        ON A.cnpj_base = F.cnpj_base 
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
                    DROP TABLE PUBLIC.tmp_empresa_geral;
                    DROP TABLE PUBLIC.tmp_numero_socios;
                    DROP TABLE PUBLIC.tmp_socio_adm;
            """
            self.cursor.execute(sql_drop_tmp)
            logger.info('tabelas temporarias dropadas')
        except Exception as e:
            logger.info(str(e))
    
    def consulta_dados_fato(self, cod_situacao_cadastral, cnae_principal, cnae_secundario):
        try:
            sql = f"""
                    SELECT  REGEXP_REPLACE
                                (cnpj, '(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})',
                                       '\1.\2.\3/\4-\5')                                AS "CNPJ",
                            matriz_filial                                               AS "Matriz/Filial",
                            INITCAP(razao_social)                                       AS "Razão Social",
                            INITCAP(nome_fantasia)                                      AS "Nome",
                            natureza_juridica 	                                        AS "Natureza Jurídica",
                            TO_CHAR(data_situacao_cadastral::timestamp, 'DD-MM-YYYY')   AS "Data Situação",
                            TO_CHAR(data_inicio_atividade::timestamp, 'DD-MM-YYYY')     AS "Data Abertura",
                            cnae_principal                                              AS "CNAE",
                            descricao_cnae                                              AS "Atividade",
                            TRIM(cnae_secundario)                                       AS "CNAE Secundário",
                            INITCAP(tipo_logradouro)                                    AS "Tipo Logradouro ",
                            INITCAP(logradouro)                                         AS "Logradouro",
                            numero                                                      AS "Número",
                            INITCAP(complemento)                                        AS "Complemento",
                            INITCAP(bairro)                                             AS "Bairro",
                            INITCAP(municipio)                                          AS "Município",
                            REGEXP_REPLACE(cep, '([0-9]{5})([0-9]{3})','\1-\2')         AS "CEP",
                            uf                                                          AS "UF",
                            pais                                                        AS "País",
                            ddd1                                                        AS "DDD",
                            telefone1                                                   AS "Telefone",
                            ddd2                                                        AS "DDD 2",
                            telefone2                                                   AS "Telefone 2",
                            LOWER(email)                                                AS "E-mail",
                            CASE
                                WHEN opcao_simples = 'N' THEN 'Não'
                                ELSE 'Sim'
                            END                                                         AS "Simples Nacional",
                            quantidade_empresa 	                                        AS "Número de empresas"
                    FROM   PUBLIC.fat_dados_empresa
                    WHERE  cnae_principal  = '{cnae_principal}'
                        AND cnae_secundario like '%{cnae_secundario}%'
                        AND codigo_situacao_cadastral = '{cod_situacao_cadastral}'
                        ;
                    """
            df = pd.read_sql(sql, self.con)
            logger.info(f'CNAE Principal: {cnae_principal}, CNAE Secundário: {cnae_secundario} e Situação Cadastral: {cod_situacao_cadastral}. Dados filtrados!')
            return df 
        except Exception as e:
            logger.info(str(e))