import json
import requests
import base64
import sqlite3
import re
import inspect
import os
import sys


def get_script_dir(follow_symlinks=True):
    if getattr(sys, 'frozen', False): # py2exe, PyInstaller, cx_Freeze
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(get_script_dir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path) + '/'


class Krokus:
    connection = sqlite3.connect(get_script_dir()+'base.db')
    cursor = connection.cursor()

    base_url = 'http://swop.krokus.ru/ExchangeBase/hs/catalog'
    user = 'МежунцРузанЕремовнаИП401104760841'
    passwd = 'Ly5!Tuqife'

    def __init__(self):
        self.init_db()
        self.headers = {
            'Authorization': f'Basic {base64.b64encode(bytes(f"{self.user}:{self.passwd}", "utf-8")).decode("utf-8")}'
        }

    def __del__(self):
        self.connection.close()

    def init_db(self):
        try:
            self.connection.execute("""
                create table items
                (
                    id             INTEGER not null,
                    sku_base       TEXT    not null,
                    category_id    TEXT,
                    category_name  TEXT,
                    brand_id       INTEGER,
                    brand_name     TEXT,
                    type           TEXT,
                    count          TEXT,
                    retail_price   TEXT,
                    purchase_price TEXT,
                    sku            TEXT
                );
            """)
            self.connection.execute("""
                create unique index id_index
            on items (id);
                """)
            print('Database initialized successfully')
        except:
            print('Database is already initialized')

    def get_all_ids(self, start_page=1, step=1, pageSize=10000):
        page = start_page
        with open('ids.txt', 'w', encoding='utf-8') as f:
            f.write('')
        while True:
            ids = []
            url = f'{self.base_url}/nomenclature?fieldSet=min&format=json&pageSize={pageSize}&page={page}'
            r = requests.post(url, headers=self.headers)
            try:
                res = r.json()['nomenclatures']
                if len(res) > 0:
                    for item in res:
                        ids.append(item['id'])
                    print(f'{url} done.')
                    with open('ids.txt', 'a', encoding='utf-8') as f:
                        f.write('\n'.join(ids))
                    page = page + step
                else:
                    print(f'Total ids: {len(ids)}')
                    with open('ids.txt', 'a', encoding='utf-8') as f:
                        f.write('\n'.join(ids))
                    return ids
                    # return 0
            except:
                print(r.text)
                break

    def filter_ids(self):
        with open('ids.txt', 'r', encoding='utf-8') as f:
            ids = f.read().split('\n')
            filtered_ids = list(set(ids))
            print(f'Input: {len(ids)}, Output: {len(filtered_ids)}')
        with open('ids.txt', 'w', encoding='utf-8') as f:
            f.write("\n".join(filtered_ids))
        print('Filtering done.')

    def get_json_by_arts(self, arts: list):
        payload = {
            "articles": arts,
            "typeOfSearch": "Артикул"
        }
        r = requests.post(f"{base_url}/getidbyarticles", headers=self.headers, json=payload)
        try:
            js_buf = r.json()
        except:
            print('ERR')
            return 1
        ids = []
        items = js_buf['result']
        for item in items:
            ids.append({"id": item['id'], "amount": 0})

        payload = {
            "goods": ids
        }

        r = requests.post(f"{self.base_url}/stockOfGoods", headers=self.headers, json=payload)
        js_buf = r.json()['stockOfGoods']
        out = []
        for item in js_buf:
            out.append({
                'articul': item['articul'],
                'price': item['price'],
                'priceBasic': item['priceBasic'],
                'stockamountTotal': item['stockamount'] + item['stockamountAdd']
            })
        try:
            return out
        except:
            return None

    def fill_db_from_list(self, lst: list):
        for i in lst:
            try:
                t = (
                    int(i['id']), i['articulElevel'], i['categoryId'], i['categoryName'], int(i['brandId']),
                    i['brandName'],
                    i['type'])
                self.cursor.execute(
                    f"""INSERT OR REPLACE INTO items 
                    (id,sku_base,category_id,category_name,brand_id,brand_name,type) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, t)
            except Exception as e:
                print(e)
        self.connection.commit()
        self.cursor.execute('SELECT COUNT(*) FROM items')
        count = int(self.cursor.fetchall()[0][0])

        print(f'{count} (SQL) of {len(lst)} (LIST) records processed')

    def load_ids_from_file(self, fname='ids.txt'):
        with open(fname, 'r', encoding='utf-8') as f:
            ids = f.read().split('\n')
        print(f'IDs before de-duplicate: {len(ids)}')
        ids = list(set(ids))
        print(f'IDs after de-duplicate: {len(ids)}')
        return ids

    def get_ids_details(self, ids: list, step=20000):
        url = f"{self.base_url}/nomenclature?fieldSet=max&pageSize={step}&page=1"
        payload = {
            "ids": ids
        }
        r = requests.post(url, headers=self.headers, json=payload)

        try:
            response = r.json().get('nomenclatures', None)
        except Exception as e:
            s = r.text
            replaced = re.sub('\[,', '[', s)
            try:
                response = json.loads(replaced)
            except Exception as e:
                print(e)
                response = None
        return response

    def fill_db_by_ids_from_file(self, fname="ids.txt", step=5000):
        ids = self.load_ids_from_file(fname)

        current_id = 0
        total_cur = 0

        pl = []
        for id in ids:
            pl.append(str(id))
            current_id += 1
            total_cur += 1
            if current_id <= step:
                continue
            # --------- Breakpoint ---------------

            current_id = 0

            response = self.get_ids_details(pl)
            if response:
                self.fill_db_from_list(response)

            pl.clear()

            print(f'Current id: {total_cur}, list length: {len(ids)}')
        self.cursor.execute('SELECT COUNT(*) FROM items')
        count = int(self.cursor.fetchall()[0][0])

        print(f'DONE! {count} of {len(ids)} records processed')

    def load_stocks_by_ids(self, ids: list):
        out = []
        for id in ids:
            out.append({"id": str(id), "amount": 0})

        payload = {
            "goods": out
        }

        r = requests.post(f"{self.base_url}/stockOfGoods", headers=self.headers, json=payload)
        try:
            js_buf = r.json()['stockOfGoods']
            print(len(js_buf))
        except:
            print(r.content)
            return None
        out = []
        if js_buf is None:
            print(r.json()['message'])
            return None
        for item in js_buf:
            q = 'UPDATE items SET sku = ?, purchase_price = ?,retail_price = ?, count = ? WHERE id = ?'
            self.cursor.execute(q,(item['articul'],item['price'],item['priceBasic'],item['stockamount'] + item['stockamountAdd'],int(item['id'])))
            out.append({
                'id':item['id'],
                'articul': item['articul'],
                'price': item['price'],
                'priceBasic': item['priceBasic'],
                'stockamountTotal': item['stockamount'] + item['stockamountAdd']
            })
        self.connection.commit()
        try:
            print(json.dumps(out, ensure_ascii=False, indent=4))
            return out
        except:
            return None

    def load_stocks(self, fname = get_script_dir()+'brands_to_parse'):
        with open(fname, 'r', encoding='utf-8') as f:
            brands = f.read().split('\n')
        placeholder = '?'
        placeholders = ', '.join(placeholder for unused in brands)
        query = 'SELECT id FROM items WHERE brand_name IN (%s)' % placeholders
        self.cursor.execute(query, brands)
        lst = [item[0] for item in self.cursor.fetchall()]
        #print(lst)
        self.load_stocks_by_ids(lst)

    def compare_ids(self):
        self.cursor.execute('SELECT id FROM items')
        sql_ids = [str(id[0]) for id in self.cursor.fetchall()]
        comp_ids = []
        count = 0
        # print(sql_ids[0:20])
        with open('ids.txt', 'r', encoding='utf-8') as f:
            ids = f.read().split('\n')
        total = len(ids)
        for id in ids:
            count += 1
            s = f'{count} ({id}) of {total} '
            if not (id in sql_ids):
                s += 'FAILED'
                comp_ids.append(id)
                print(s)

        if len(comp_ids) > 0:
            with open('compids.txt', 'w', encoding='utf-8') as f:
                f.write('\n'.join(comp_ids))
        print(f'DONE! Total failed: {len(comp_ids)}')

    def get_brands(self):
        self.cursor.execute('SELECT brand_name FROM items;')
        lst_br = [i[0] for i in self.cursor.fetchall()]
        brands = list(set(lst_br))
        brands_final = []
        total = len(brands)
        for brand in brands:
            self.cursor.execute(f"SELECT COUNT(*) FROM items WHERE brand_name LIKE '%{brand}%'")
            count = self.cursor.fetchall()[0][0]
            brands_final.append(f'{brand} | {count}')
            print(f'{brand} | {count}')
        with open('brands.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(brands_final))
        print(f'Total {total} brands')

    def update_db(self):
        self.get_all_ids()
        self.fill_db_by_ids_from_file()


if __name__ == "__main__":
    print(get_script_dir())
    krokus = Krokus()
    krokus.update_db()
    krokus.load_stocks()
    del krokus
