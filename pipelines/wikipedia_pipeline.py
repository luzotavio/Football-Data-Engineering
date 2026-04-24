import json
from datetime import datetime

import pandas as pd

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url):
    import requests

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print('getting wikipedia page...', url)

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f'An error occurred: {e}')
        raise e


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all("table", {"class": ["wikitable", "sortable"]})

    if len(tables) > 1:
        table = tables[1]
        table_rows = table.find_all('tr')
        return table_rows
    else:
        print("Tabela não encontrada!")
        return []

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp','')
    text = text.replace('\xad', '')
    if ' ♦' in text:
        text = text.split(' ♦')[0]
    if '[' in text:
        text = text.split('[')[0]
    if ' (formerly' in text:
        text = text.split('(formerly')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    print(rows)

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split('//')[1] if tds[5].find('img') else 'no_image',
            'home_team': clean_text(tds[6].text)
        }

        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return 'ok'


def get_lat_long(query):
    import time
    import requests
    import os

    API_KEY = os.environ.get('OPENCAGE_API_KEY')
    url = f'https://api.opencagedata.com/geocode/v1/json?q={query}&key={API_KEY}'

    time.sleep(1)

    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        if data['results']:
            lat = data['results'][0]['geometry']['lat']
            lng = data['results'][0]['geometry']['lng']
            return lat, lng
        return None

    except Exception as e:
        print(f'Erro ao geocodificar "{query}": {e}')
        return None


def transform_wikipedia_data(**kwargs):

    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)

    # Tenta geocodificar pelo nome do estádio + país (coordenada exata)
    stadiums_df['location'] = stadiums_df.apply(
        lambda row: get_lat_long(f"{row['stadium']}, {row['country']}"), axis=1
    )

    stadiums_df['images'] = stadiums_df['images'].apply(
        lambda x: x if x not in ['NO IMAGE', '', None] else NO_IMAGE
    )
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # Para coordenadas duplicadas, tenta pela cidade + país
    duplicate_mask = stadiums_df.duplicated(subset=['location'], keep='first')
    stadiums_df.loc[duplicate_mask, 'location'] = stadiums_df[duplicate_mask].apply(
        lambda row: get_lat_long(f"{row['city']}, {row['country']}"), axis=1
    )

    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return 'ok'

def write_wikipedia_data(**kwargs):
    import os

    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now()) + '_' + str(datetime.now().time()).replace(':',
                                                                                                     '_') + '.csv')

    data.to_csv('abfs://footballdataeng@footballdataenglo.dfs.core.windows.net/data/' + file_name,
                storage_options={
                'account_key': os.environ.get('AZURE_ACCOUNT_KEY')
                }, index=False)

