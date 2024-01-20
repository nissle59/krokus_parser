import sqlite3
import json
import inspect
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import warnings

warnings.filterwarnings("ignore")

log_file = 'getter.log'

logging.basicConfig(level=logging.INFO, filename=log_file, filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
args = sys.argv[1:]



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(log_file, maxBytes=2000000, backupCount=5)
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))


def get_script_dir(follow_symlinks=True):
    if getattr(sys, 'frozen', False): # py2exe, PyInstaller, cx_Freeze
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(get_script_dir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path) + '/'


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_json(dbfile = get_script_dir()+'base.db'):
    print(dbfile)
    connection = sqlite3.connect(dbfile)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    # placeholder = '?'
    # placeholders = ', '.join(placeholder for unused in l)
    query = "SELECT sku,purchase_price,retail_price,count FROM items WHERE sku IS NOT NULL"
    cursor.execute(query)
    result = [dict(row) for row in cursor.fetchall()]
    connection.close()
    logger.info(f'{len(result)} requested from DB')
    return result


if __name__ == "__main__":
    js = get_json()
    print(json.dumps(js,ensure_ascii=False,indent=4))
