import pandas as pd
from datetime import date
import logging
import sys
sys.path.append("..")

from database.database import Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Leads:
    def __init__(self):
        self.df = Database()

    def gera_leads(self):
        situacao_cadastral = input('01 - NULA | 02 - ATIVA | 03 - SUSPENSA | 04 - INAPTA | 08 - BAIXADA: ')
        cnae_principal = input('Digite o CNAE principal (somente números): ')
        cnae_secundario = input('Digite o CNAE secundario (somente números): ')

        df_filtrado = self.df.consulta_dados_fato(situacao_cadastral, cnae_principal, cnae_secundario)
        return df_filtrado
    
    def export_leads(self):
        try:
            data_atual = date.today()
            data_formatada = str(data_atual.strftime('%d-%m-%Y'))
            df = self.gera_leads()
            
            logger.info('Exportando leads em potencial')
            writer = pd.ExcelWriter(f'data_leads/Leads-{data_formatada}.xlsx', engine='xlsxwriter')
            df.to_excel(writer, sheet_name='Potenciais_Leads', index=False)
            writer.save()
            logger.info('Leads exportados com sucesso!')
        except Exception as e:
            logger.info(str(e))
            logger.info('ERRO!!!')
