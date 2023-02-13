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

    def cria_tabela_empresas(self):
        try:
            sql = f"""
                        CREATE TABLE IF NOT EXISTS public.empresa 
                            (
                                code varchar(100) NULL,
                                status varchar(100) NULL,
                                message varchar(100) NULL,
                                cnpj varchar(100)PRIMARY KEY,
                                tipo varchar(100) NULL,
                                porte varchar(100) NULL,
                                situacao varchar(100) NULL,
                                abertura varchar(100) NULL,
                                nome varchar(100) NULL,
                                fantasia varchar(100) NULL,
                                atividade_principal varchar(100) NULL,
                                atividade_principal_code varchar(100) NULL,
                                atividade_principal_text varchar(100) NULL,
                                atividades_secundarias varchar(100) NULL,
                                atividades_secundarias_code varchar(100) NULL,
                                atividades_secundarias_text varchar(100) NULL,
                                natureza_juridica varchar(100) null,
                                logradouro varchar(100) NULL,
                                numero varchar(100) NULL,
                                complemento varchar(100) NULL,
                                cep varchar(100) NULL,
                                bairro varchar(100) null,
                                municipio varchar(100) NULL,
                                uf varchar(100) NULL,
                                email varchar(100) null,
                                telefone varchar(100) NULL,
                                efr varchar(100) NULL,
                                data_situacao varchar(100) NULL,
                                motivo_situacao varchar(100) NULL,
                                situacao_especial varchar(100) NULL,
                                data_situacao_especial varchar(100) NULL,
                                capital_social varchar(100) NULL,
                                qsa varchar(100) NULL,
                                qsa_nome varchar(100) NULL,
                                qsa_qual varchar(100) null,
                                qsa_pais_origem varchar(100) NULL,
                                qsa_nome_rep_legal varchar(100) NULL,
                                qsa_qual_rep_legal varchar(100) NULL,
                                extra varchar(100) NULL,
                                ibge varchar(100) NULL,
                                ibge_codigo_municipio varchar(100) null,
                                ibge_codigo_uf varchar(100) null,
                                cnpjs_do_grupo varchar(100) NULL,
                                cnpj_outro varchar(100) null,
                                uf_outro varchar(100) null,
                                tipo_outro varchar(100) NULL,
                                created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
                            );
                    """
            self.cursor.execute(sql)
            logger.info('Tabela empresa criada')            
        except Exception as e:
            logger.info(str(e))