import sys
import pathlib

path_src = str(pathlib.Path(__file__).parent.resolve()) + '/../'
sys.path.append(path_src)

from cnpj.get_cnpj_public_data import DB_CNPJ

USER = 'monkey'
PASSWORD = 'monkey'
SERVER_IP = 'localhost:5432' # Server IP + Port
SCHEMA = 'public'
obj = DB_CNPJ(USER, PASSWORD, SERVER_IP, SCHEMA)

obj.show_files()

# For a single update file per file
# Type the name of the file you want to download
obj.download_files()
# obj.download_files(['Socios9.zip'])

obj.uploaded = []
obj.upload_to_postgresql(first_upload_truncate=True)
obj.index_db()

