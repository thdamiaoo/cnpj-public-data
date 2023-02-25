import sys
import pathlib

path_src = str(pathlib.Path(__file__).parent.resolve()) + '/../'
sys.path.append(path_src)

from validate_cnpj.validate_cnpj import Validation
from database.database import Database

Database().cria_tabela_fato()

# Validation().trata_resposta()