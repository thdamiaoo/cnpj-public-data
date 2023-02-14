from pathlib import Path
import pathlib
import logging
import sys
import requests
import json
from time import sleep
import pandas as pd

path_src = str(pathlib.Path(__file__).parent.resolve()) + '/..'
sys.path.append(path_src)

from generate_valid_cnpj.generate_cnpj import cnpj
from database.database import Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
path_src = str(Path(__file__).parent.resolve())


class Validation:
    url = 'https://www.receitaws.com.br/v1/cnpj/'

    def __init__(self):
        self.db = Database()
        self.session = requests.Session()

    def valida_cnpj(self):
        try:
            self.generate_cnpj = cnpj.generate_cnpj()
            url = self.url + '49369670000179' # self.generate_cnpj
            querystring = {"token":"XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX","cnpj":"06990590000123","plugin":"RF"}        
            response = self.session.get(url, params=querystring)
            self.resp = json.loads(response.text)
            status = self.resp['status']

            if status == "OK":
                logger.info(f'>>>>>>>>>>> CNPJ: {self.generate_cnpj} status: {status} - comitado <<<<<<<<<')
                return self.resp
            else:
                logger.info(f'>>>>>>>>>>> CNPJ: {self.generate_cnpj} status: {status} - descartado <<<<<<<<<')
                # sleep(10.0)
                # self.valida_cnpj()
                    # retry = True
                    # attempts = 1

                    # while retry:
                    #     new_generate_cnpj = cnpj.generate_cnpj()
                    #     sleep(10)                
                    #     response = self.session.get(url, params=querystring)
                    
                    #     print('-------------------------', response)
                    #     status = resp['status']
                    #     message = resp['message']

                    #     if status != 'OK':
                    #     # if status == 'ERROR' and message == 'not in cache':
                    #         logger.info(f'nova valicao CNPJ: {new_generate_cnpj} status: {status} - attempt {attempts}')
                    #         retry = True
                    #         attempts = attempts + 1

                    #     else:
                    #         logger.info(f'>>>>>>>>>>> CNPJ: {new_generate_cnpj} status: {status} - comitado apos retry <<<<<<<<<')
                    #         retry = False            
            sleep(10.0)
            self.valida_cnpj()
        except Exception as e:
            logger.info(str(e))
        
    def trata_resposta(self):
        resp = self.valida_cnpj()
        cnpj = '49369670000179'
        df = pd.json_normalize( resp, 
                                record_path=['atividades_secundarias'], 
                                meta=[
                                        'cnpj',
                                        'data_situacao',
                                        'motivo_situacao',
                                        'tipo',
                                        'nome',
                                        'fantasia',
                                        'porte',
                                        'natureza_juridica',
                                        'abertura',
                                        'email',
                                        'qsa',
                                        'situacao',
                                        'logradouro',
                                        'numero',
                                        'municipio',
                                        'bairro',
                                        'uf',
                                        'telefone',
                                        'cep',
                                        'complemento',
                                        'efr',
                                        'situacao_especial',
                                        'data_situacao_especial', 
                                        'atividade_principal',
                                        'capital_social',
                                        'extra',
                                        'billing',
                                        'ultima_atualizacao',
                                        'status'
                                ]
                            )
        df.rename(columns={'text':'descricao', 'code':'codigo'}, inplace=True)
        # print(df)
        # exit()
        logger.info('dados preparados da serem inseridos')
        self.db.insere_dados_empresa(df, cnpj)



