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

        # print(pg_connection)
        # exit()
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
    
    def cria_fato(self):
        try:
            sql = f"""
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
        
            self.cursor.execute(sql)
            logger.info('tabelas temp criadas')
        except Exception as e:
            logger.info(str(e))