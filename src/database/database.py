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
        self.cria_tabela_empresas()
        self.dados_brutos()

    def cria_tabela_empresas(self):
        try:
            sql = f"""
                        CREATE TABLE IF NOT EXISTS public.empresa 
                            (
                                id serial4 NOT NULL,
                                codigo text NULL,
                                descricao text NULL,
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
            self.cursor.execute(sql)
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