



import pandas as pd
import os


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

def extract_wikipedia_data(**kwargs):
    url=kwargs['url']
    html=get_wikipedia_page(url)
    rows=get_wikipedia_data(html)

    data=[]
    
    for i in range(1,len(rows)):
        tds=rows[i].find_all('td')
        values={
            'rank':i,
            'stadium':tds[0].text,
            'capacity':tds[1].text,
            'region':tds[2].text,
            'country':tds[3].text,
            'city':tds[4].text,
            'images':'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team':tds[6].text,
        }

        data.append(values)
    data_df=pd.DataFrame(data)    
    output_file = output_file = "/tmp/output.csv"

    # Écrire les données dans un fichier CSV
    data_df.to_csv(output_file, index=False)
    print(f"Fichier écrit avec succès : {output_file}")
    
    return data
