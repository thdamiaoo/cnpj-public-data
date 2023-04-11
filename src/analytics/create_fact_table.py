import sys
import pathlib

path_src = str(pathlib.Path(__file__).parent.resolve()) + '/../'
sys.path.append(path_src)

from database.database import Database

db = Database()
db.cria_tabela_fato()