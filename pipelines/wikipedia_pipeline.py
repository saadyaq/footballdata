from geopy.exc import GeocoderUnavailable
from geopy.geocoders.nominatim import Nominatim
import pandas as pd
import os
import json
NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    
    import requests

    print("Getting Wikipedia page:", url)

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        html = response.text
        # Vérifie si le tableau est présent
        if "wikitable" in html:
            print("Le tableau est présent dans le HTML.")
        else:
            print("Le tableau n'est PAS présent dans le HTML.")
        
        return html
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération de la page : {e}")
        return None

        

def get_wikipedia_data(html):
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html, 'html.parser')

    # Chercher le tableau principal avec les bonnes classes
    table = soup.find("table", {"class": lambda value: value and "wikitable sortable sticky-header" in value})
    if not table:
        raise ValueError("Le tableau principal n'a pas été trouvé.")

    # Trouver toutes les lignes dans <tbody> pour éviter les en-têtes
    table_body = table.find("tbody")
    if not table_body:
        raise ValueError("Le corps du tableau (<tbody>) n'a pas été trouvé.")

    rows = table_body.find_all("tr")
    print(f"Nombre de lignes trouvées dans <tbody> : {len(rows)}")
    return rows


def clean_text(text):
    text=str(text).strip()
    text=text.replace('&nbsp',"")
    if text.find(' ♦'):
        text=text.split(' ♦')[0]
    if text.find('[') !=-1:
        text=text.split('[')[0]
    if text.find(' (formerly') !=-1:
        text=text.split(' (formerly')[0]
    

    return text.replace('\n','')



def extract_wikipedia_data(**kwargs):
    url=kwargs['url']
    html=get_wikipedia_page(url)
    rows=get_wikipedia_data(html)

    data=[]
    
    for i in range(1,len(rows)):
        tds=rows[i].find_all('td')
        values={
            'rank':i,
            'stadium':clean_text(tds[0].text),
            'capacity':clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region':clean_text(tds[2].text),
            'country':clean_text(tds[3].text),
            'city':clean_text(tds[4].text),
            'images':'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team':clean_text(tds[6].text),
        }

        data.append(values)
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    print(f"JSON généré pour XCom : {json_rows}")  # Log des données générées
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    print("Données poussées dans XCom avec succès.")

    return "OK"
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_lat_long(country,city):
    geolocator=Nominatim(user_agent='geoapifoot')
    location=geolocator.geocode(f'{city},{country}')
    try:
        location = geolocator.geocode(f'{city},{country}')
        if location:
            return location.latitude, location.longitude
        else:
            return None
    except GeocoderUnavailable:
        return None
def transform_wikipedia_data(**kwargs):

    data=kwargs['ti'].xcom_pull(key='rows',task_ids='extract_data_from_wikipedia')
    if not data:
        raise ValueError("Les données extraites sont vides. Vérifiez la tâche 'extract_wikipedia_data' et les données XCom.")

    print(f"Données extraites de XCom : {data}")
    data=json.loads(data)
    stadium_df=pd.DataFrame(data)
    stadium_df['images']=stadium_df['images'].apply(lambda x: x if x not in ['NO_IMAGE','',None] else NO_IMAGE)
    stadium_df['capacity']=stadium_df['capacity'].astype(int)
    stadium_df['location']=stadium_df.apply(lambda x: get_lat_long(x['country'],x['city']),axis=1)

    #Duplicates
    duplicates=stadium_df[stadium_df.duplicated(['location'])]

    duplicates['location']=duplicates.apply(lambda x: get_lat_long(x['country'],x['city']),axis=1)
    stadium_df.update(duplicates)

    kwargs['ti'].xcom_push(key='rows',value=stadium_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime

    data=kwargs['ti'].xcom_pull(key='rows',task_ids='transform_wikipedia_data')
    data=json.loads(data)
    data=pd.DataFrame(data)

    file_name=("stadium_cleaned_"+str(datetime.now().date())+"_"+ str(datetime.now().time()).replace(":","_")+".csv")
    # Define the output file path in the /tmp directory
    output_file = os.path.join("/tmp", file_name)

    # Save the DataFrame to the output file
    data.to_csv(output_file, index=False)

    print(f"File saved successfully to: {output_file}")
    stadium_cleaned_2025-01-11_19_09_20.624089.csv