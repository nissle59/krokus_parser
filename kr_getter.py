import sqlite3
import json


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_json_by_sku_list(l:list, dbfile = 'base.db'):
    connection = sqlite3.connect(dbfile)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    placeholder = '?'
    placeholders = ', '.join(placeholder for unused in l)
    query = 'SELECT sku, "purchase_price", "retail_price ", "count" FROM items WHERE sku IS NOT NULL AND items.sku IN (%s)' % placeholders
    cursor.execute(query, l)
    result = [dict(row) for row in cursor.fetchall()]
    connection.close()
    return result


def get_json_by_brand_list(l:list, dbfile = 'base.db'):
    connection = sqlite3.connect(dbfile)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    placeholder = '?'
    placeholders = ', '.join(placeholder for unused in l)
    query = "SELECT sku,purchase_price,retail_price,count FROM items WHERE sku IS NOT NULL AND items.brand_name IN (%s)" % placeholders
    cursor.execute(query, l)
    result = [dict(row) for row in cursor.fetchall()]
    connection.close()
    return result


def get_json_from_brands_file(fname = 'brands_to_parse'):
    with open(fname, 'r') as f:
        brlist = f.read().split('\n')
    return get_json_by_brand_list(brlist)


if __name__ == "__main__":
    js = get_json_from_brands_file('br_test')
    print(json.dumps(js,ensure_ascii=False,indent=4))