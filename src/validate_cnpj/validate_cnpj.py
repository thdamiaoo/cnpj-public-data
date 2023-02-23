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
            url = self.url +  self.generate_cnpj
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
        try:
            resp = self.valida_cnpj()
            df_atividade_principal = pd.json_normalize( resp, 
                                    record_path=['atividade_principal'], 
                                    meta=['cnpj'])                                
            df_atividade_principal.rename(columns = {'code':'cod_atividade_principal', 'text':'desc_atividade_principal'}, inplace = True)
            df_atividades_secundarias = pd.json_normalize( resp, 
                                    record_path=['atividades_secundarias'], 
                                    meta=['cnpj'])
            df_atividades_secundarias.rename(columns = {'code':'cod_atividade_secundaria', 'text':'desc_atividade_secundaria'}, inplace = True)
            df_atividades = pd.merge(df_atividade_principal, df_atividades_secundarias, on= 'cnpj', how='inner')
            df_qsa = pd.json_normalize( resp, 
                                    record_path=['qsa'], 
                                    meta=['cnpj', 'data_situacao', 'motivo_situacao', 'tipo', 'fantasia', 'porte', 'natureza_juridica', 'abertura', 'email',
                                        'situacao', 'logradouro', 'numero', 'municipio', 'bairro', 'uf', 'telefone', 'cep', 'complemento', 'efr', 'situacao_especial',
                                        'data_situacao_especial', 'capital_social', 'ultima_atualizacao', 'status'])
            df_qsa.rename(columns = {'nome':'nome_socio', 'qual':'qualificacao_socio'}, inplace = True)                                
            df_merge = pd.merge(df_atividades, df_qsa, on='cnpj', how='inner')
            print(df_merge)
            return df_merge
        except Exception as e:
            logger.info(str(e))
    
    # def insere_dadado
        




                                        

                            
            
        
        
        




