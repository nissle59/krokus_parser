import json
import requests
import base64
import sqlite3
import re

connection = sqlite3.connect('base.db')
cursor = connection.cursor()


base_url = 'http://swop.krokus.ru/ExchangeBase/hs/catalog'
user = 'МежунцРузанЕремовнаИП401104760841'
passwd = 'Ly5!Tuqife'


def get_all_ids(user,passwd,start_page=1, step=1):
    headers = {
        'Authorization': f'Basic {base64.b64encode(bytes(f"{user}:{passwd}", "utf-8")).decode("utf-8")}'
    }
    page = start_page
    with open('ids.txt', 'w') as f:
        f.write('')
    ids = []
    while True:
        ids = []
        url = f'{base_url}/nomenclature?fieldSet=min&format=json&pageSize=10000&page={page}'
        r = requests.post(url, headers=headers)
        try:
            res = r.json()['nomenclatures']
            if len(res) > 0:
                #fill_db_from_list(res)
                for item in res:
                    ids.append(item['id'])
                print(f'{url} done.')
                with open('ids.txt','a') as f:
                    f.write('\n'.join(ids))
                page = page + step
            else:
                print(f'Total ids: {len(ids)}')
                with open('ids.txt','a') as f:
                    f.write('\n'.join(ids))
                return ids
                #return 0
        except:
            print(r.text)
            break


def filter_ids():
    with open('ids.txt','r') as f:
        ids = f.read().split('\n')
        filtered_ids = list(set(ids))
        print(f'Input: {len(ids)}, Output: {len(filtered_ids)}')
    with open('ids.txt','w') as f:
        f.write("\n".join(filtered_ids))
    print('Filtering done.')


def get_json_by_arts(arts:list,user,passwd):
    headers = {
        'Authorization': f'Basic {base64.b64encode(bytes(f"{user}:{passwd}","utf-8")).decode("utf-8")}'
    }
    payload = {
        "articles": arts,
        "typeOfSearch": "Артикул"
    }
    r = requests.post(f"{base_url}/getidbyarticles", headers=headers, json=payload)
    try:
        js_buf = r.json()
    except:
        print('ERR')
        return 1
    ids = []
    items = js_buf['result']
    for item in items:
        ids.append({"id":item['id'],"amount":0})

    payload = {
        "goods": ids
    }

    r = requests.post(f"{base_url}/stockOfGoods", headers=headers, json=payload)
    js_buf = r.json()['stockOfGoods']
    out = []
    for item in js_buf:
        out.append({
            'articul':item['articul'],
            'price':item['price'],
            'priceBasic':item['priceBasic'],
            'stockamountTotal':item['stockamount']+item['stockamountAdd']
        })
    try:
        return out
    except:
        return None


def fill_db_from_list(lst:list):
    for i in lst:
        try:
            cursor.execute(
                'INSERT INTO items (id,sku,category_id,category_name,brand_id,brand_name,type) VALUES (?, ?, ?, ?, ?, ?, ?)',
                (
                int(i['id']), i['articulElevel'], i['categoryId'], i['categoryName'], int(i['brandId']), i['brandName'],
                i['type']))
            connection.commit()
        except:
            print(f'Item with id = {i["id"]} already exists, try to update..')
            try:
                cursor.execute(
                    'UPDATE items SET sku = ?,category_id = ?,category_name = ?,brand_id = ?,brand_name = ?,type = ? WHERE id = ?',
                    (i['articulElevel'], i['categoryId'], i['categoryName'], int(i['brandId']), i['brandName'],
                     i['type'], int(i['id'])))
                connection.commit()
            except:
                print(f'Can\'t update item with id = {i["id"]}')
    cursor.execute('SELECT COUNT(*) FROM items')
    count = int(cursor.fetchall()[0])

    print(f'DONE! {count} of {len(lst)} records processed')

def fill_db_by_ids_from_file(user,passwd,fname="ids.txt",step=5000):
    headers = {
        'Authorization': f'Basic {base64.b64encode(bytes(f"{user}:{passwd}", "utf-8")).decode("utf-8")}'
    }
    with open(fname,'r') as f:
        ids = f.read().split('\n')
    print(f'IDs before de-duplicate: {len(ids)}')
    ids = list(set(ids))
    print(f'IDs after de-duplicate: {len(ids)}')
    current_id = 0
    total_cur = 0
    items = []
    pl = []
    for id in ids:
        pl.append(str(id))
        current_id = current_id + 1
        total_cur += 1
        if current_id < step:
            continue
        else:
            current_id = 0
            url = f"{base_url}/nomenclature?fieldSet=max&pageSize={step}&page=1"
            payload = {
                "ids":pl
            }
            r = requests.post(url, headers=headers, json=payload)
            pl.clear()
            try:
                response = r.json().get('nomenclatures',None)
            except Exception as e:
                s = r.text
                replaced = re.sub('\[,','[',s)
                try:
                    response = json.loads(replaced)
                except Exception as e:
                    print(e)
                    response = None
            if response:
                for i in response:
                    try:
                        cursor.execute('INSERT INTO items (id,sku,category_id,category_name,brand_id,brand_name,type) VALUES (?, ?, ?, ?, ?, ?, ?)',
                                       (int(i['id']), i['articulElevel'], i['categoryId'], i['categoryName'], int(i['brandId']),i['brandName'],i['type']))
                        connection.commit()
                    except:
                        #print(i)
                        print(f'Item with id = {i["id"]} already exists, try to update..')
                        try:
                            cursor.execute('UPDATE items SET sku = ?,category_id = ?,category_name = ?,brand_id = ?,brand_name = ?,type = ? WHERE id = ?', (i['articulElevel'], i['categoryId'], i['categoryName'], int(i['brandId']),i['brandName'],i['type'],int(i['id'])))
                            connection.commit()
                        except:
                            print(f'Can\'t update item with id = {i["id"]}')

            print(f'Current id: {total_cur}, list length: {len(items)}')
    cursor.execute('SELECT COUNT(*) FROM items')
    count = int(cursor.fetchall()[0][0])

    print(f'DONE! {count} of {len(ids)} records processed')


def get_json_by_ids(ids:list,user,passwd):
    headers = {
        'Authorization': f'Basic {base64.b64encode(bytes(f"{user}:{passwd}","utf-8")).decode("utf-8")}'
    }
    out = []
    for id in ids:
        out.append({"id": id, "amount": 0})
    #print(json.dumps(out,ensure_ascii=False,indent=4))
    payload = {
        "goods": out
    }
    #print(json.dumps(payload, ensure_ascii=False, indent=4))

    r = requests.post(f"{base_url}/stockOfGoods", headers=headers, json=payload)
    try:
        js_buf = r.json()['stockOfGoods']
    except:
        print(r.content)
        return None
    out = []
    if js_buf is None:
        print(r.json()['message'])
        return None
    for item in js_buf:
        out.append({
            'articul':item['articul'],
            'price':item['price'],
            'priceBasic':item['priceBasic'],
            'stockamountTotal':item['stockamount']+item['stockamountAdd']
        })
    try:
        print(json.dumps(out, ensure_ascii=False, indent=4))
        return out
    except:
        return None


def compare_ids():
    cursor.execute('SELECT id FROM items')
    sql_ids = [str(id[0]) for id in cursor.fetchall()]
    comp_ids = []
    count = 0
    #print(sql_ids[0:20])
    with open('ids.txt', 'r') as f:
        ids = f.read().split('\n')
    total = len(ids)
    for id in ids:
        count += 1
        s = f'{count} ({id}) of {total} '
        if not(id in sql_ids):
            s += 'FAILED'
            comp_ids.append(id)
            print(s)

    if len(comp_ids)>0:
        with open('compids.txt','w') as f:
            f.write('\n'.join(comp_ids))
    print(f'DONE! Total failed: {len(comp_ids)}')


def get_brands():
    cursor.execute('SELECT brand_name FROM items;')
    lst_br = [i[0] for i in cursor.fetchall()]
    brands = list(set(lst_br))
    brands_final = []
    total = len(brands)
    for brand in brands:
        cursor.execute(f"SELECT COUNT(*) FROM items WHERE brand_name LIKE '%{brand}%'")
        count = cursor.fetchall()[0][0]
        brands_final.append(f'{brand} | {count}')
        print(f'{brand} | {count}')
    with open('brands.txt','w') as f:
        f.write('\n'.join(brands_final))
    print(f'Total {total} brands')


if __name__ == "__main__":
    #multi_threaded_load(user,passwd, 5)
    # get_all_ids(user, passwd)
    # filter_ids()
    # fill_db_by_ids_from_file(user,passwd)
    #print(json.dumps(get_json_by_arts(['ATN000143'],user,passwd),ensure_ascii=False,indent=4))
    # items = get_json_by_ids_from_file('ids.txt',user,passwd)
    # with open('js.json','w') as f:
    #     f.write(items)
    #fill_db_by_ids_from_file(user,passwd)
    #compare_ids()
    #fill_db_by_ids_from_file(user, passwd, 'compids.txt', 50)
    get_brands()
    connection.close()