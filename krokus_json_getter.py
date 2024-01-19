import sqlite3
import json


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_json(dbfile = 'base.db'):
    connection = sqlite3.connect(dbfile)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    placeholder = '?'
    placeholders = ', '.join(placeholder for unused in l)
    query = "SELECT sku,purchase_price,retail_price,count FROM items WHERE sku IS NOT NULL"
    cursor.execute(query)
    result = [dict(row) for row in cursor.fetchall()]
    connection.close()
    return result


if __name__ == "__main__":
    js = get_json('br_test')
    print(json.dumps(js,ensure_ascii=False,indent=4))
