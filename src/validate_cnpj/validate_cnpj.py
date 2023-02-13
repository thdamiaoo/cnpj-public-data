from pathlib import Path
import pathlib
import logging
import sys
import requests
import json
from time import sleep

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
            generate_cnpj = cnpj.generate_cnpj()
            url = self.url + generate_cnpj
            querystring = {"token":"XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX","cnpj":"06990590000123","plugin":"RF"}        
            response = self.session.get(url, params=querystring)
            self.resp = json.loads(response.text)
            status = self.resp['status']

            if status == "OK":
                logger.info(f'>>>>>>>>>>> CNPJ: {generate_cnpj} status: {status} - comitado <<<<<<<<<')
                print(self.resp)
                return self.resp
            else:
                logger.info(f'>>>>>>>>>>> CNPJ: {generate_cnpj} status: {status} - descartado <<<<<<<<<')
                sleep(10)
                self.valida_cnpj()
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
            self.valida_cnpj()
        except Exception as e:
            logger.info(str(e))
    
    def insert_database(self):
        dados_empresa = self.valida_cnpj()
        print(dados_empresa)
    

