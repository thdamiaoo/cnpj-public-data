import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ConfigurationProject:

    def __init__(self):
        self.config = os.environ

    def get_envs(self):
        return self.config

    def get_postgres_connection(self):
        try:
            string_connection = 'localhost://monkey:monkey@monkey_business:5432'
            logger.info(f'String de conexão: {string_connection}')
            return string_connection
        except Exception as e:
            logger.info(str(e))
            logger.info('ERRO')