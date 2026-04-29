import json
from datetime import datetime
import pandas as pd

# Imagem padrão para quando o estádio não possui foto na Wikipedia
NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url):
    """
    Realiza a requisição HTTP para obter o HTML da página da Wikipedia.
    Utiliza headers de User-Agent para evitar bloqueios simples.
    """
    import requests

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print('Buscando página da Wikipedia...', url)

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Lança erro para status codes 4xx/5xx
        return response.text
    except requests.exceptions.RequestException as e:
        print(f'Erro na requisição: {e}')
        raise e


def get_wikipedia_data(html):
    """
    Localiza a tabela específica de estádios dentro do HTML retornado.
    A tabela alvo é a segunda 'wikitable sortable' da página.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all("table", {"class": ["wikitable", "sortable"]})

    if len(tables) > 1:
        table = tables[1] # Seleciona a segunda tabela da lista
        table_rows = table.find_all('tr')
        return table_rows
    else:
        print("Tabela alvo não encontrada!")
        return []

def clean_text(text):
    """
    Realiza a limpeza de strings removendo caracteres especiais, 
    tags residuais, referências bibliográficas (ex: [1]) e textos auxiliares.
    """
    text = str(text).strip()
    text = text.replace('&nbsp','')
    text = text.replace('\xad', '')
    
    # Remove marcadores e notas entre colchetes ou parênteses
    if ' ♦' in text:
        text = text.split(' ♦')[0]
    if '[' in text:
        text = text.split('[')[0]
    if ' (formerly' in text:
        text = text.split('(formerly')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    """
    Função principal de extração. Itera sobre as linhas da tabela, 
    mapeia as colunas e envia os dados brutos para o XCom.
    """
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    # Começa do 1 para pular o cabeçalho (th)
    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        if not tds: continue # Pula linhas vazias
        
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            # Tenta extrair a URL da imagem ou define 'no_image' se não houver
            'images': 'https://' + tds[5].find('img').get('src').split('//')[1] if tds[5].find('img') else 'no_image',
            'home_team': clean_text(tds[6].text)
        }

        data.append(values)

    # Persiste os dados extraídos no XCom para a próxima task
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return 'ok'


def get_lat_long(query):
    """
    Consulta a API OpenCage para obter coordenadas geográficas.
    Inclui um delay de 1s para respeitar limites de taxa (rate limits).
    """
    import time
    import requests
    import os

    API_KEY = os.environ.get('OPENCAGE_API_KEY')
    url = f'https://api.opencagedata.com/geocode/v1/json?q={query}&key={API_KEY}'

    time.sleep(1) # Boas práticas para APIs gratuitas

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
    """
    Realiza o tratamento de tipos, enriquecimento geográfico e 
    lógica de fallback para geolocalização.
    """
    # Recupera dados da task de extração via XCom
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)

    # Passo 1: Busca exata (Estádio + País)
    stadiums_df['location'] = stadiums_df.apply(
        lambda row: get_lat_long(f"{row['stadium']}, {row['country']}"), axis=1
    )

    # Tratamento de imagens e conversão de capacidade
    stadiums_df['images'] = stadiums_df['images'].apply(
        lambda x: x if x not in ['NO IMAGE', '', None, 'no_image'] else NO_IMAGE
    )
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # Passo 2: Lógica de Fallback para coordenadas nulas ou duplicadas
    # Tenta geocodificar apenas pela Cidade + País para maior assertividade em falhas
    duplicate_mask = stadiums_df.duplicated(subset=['location'], keep='first') | stadiums_df['location'].isna()
    
    stadiums_df.loc[duplicate_mask, 'location'] = stadiums_df[duplicate_mask].apply(
        lambda row: get_lat_long(f"{row['city']}, {row['country']}"), axis=1
    )

    # Envia o dataframe processado para a task de escrita
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return 'ok'

def write_wikipedia_data(**kwargs):
    """
    Escreve o resultado final diretamente no Azure Data Lake Storage Gen2.
    Gera um arquivo CSV com timestamp único para evitar sobrescrita.
    """
    import os

    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(data)
    data = pd.DataFrame(data)

    # Geração de nome único: stadium_cleaned_YYYY-MM-DD_HH_MM_SS.csv
    file_name = ('stadium_cleaned_' + str(datetime.now().date()) + '_' + 
                 str(datetime.now().time()).replace(':', '_').split('.')[0] + '.csv')

    # Conexão via protocolo ABFS (Azure Blob File System)
    data.to_csv('abfs://footballdataeng@footballdataenglo.dfs.core.windows.net/data/' + file_name,
                storage_options={
                    'account_key': os.environ.get('AZURE_ACCOUNT_KEY')
                }, index=False)
