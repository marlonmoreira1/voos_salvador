import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import zipfile
from unidecode import unidecode
import uuid
import pyodbc
from io import BytesIO
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = Options()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--incognito')
options.add_argument('--headless')
driver = webdriver.Chrome(r'chrome\chromedriver-win64\chromedriver.exe', options=options)

arrivals_url = 'https://www.flightradar24.com/data/airports/ssa/arrivals'
departures_url = 'https://www.flightradar24.com/data/airports/ssa/departures'

def obter_voos(url):
    import time
    url = url
    driver.get(url)
    load_more_button = driver.find_element(By.XPATH, "//button[@class='btn btn-table-action btn-flights-load']")
    for _ in range(2):
        load_more_button.click()
        time.sleep(10)
    time.sleep(10)
    element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'table-condensed') and contains(@class, 'table-hover') and contains(@class, 'data-table')]"))
        )
    html_content = element.get_attribute('outerHTML')

    
    soup = BeautifulSoup(html_content, 'html.parser')

    
    table = soup.find('table', class_='table table-condensed table-hover data-table m-n-t-15')
    flights = []
    
    if table:
        rows = table.find('tbody').find_all('tr')

        
        for row in rows:
            columns = row.find_all('td')
            if len(columns) > 1:

                time = columns[0].get_text(strip=True)
                flight = columns[1].get_text(strip=True)
                origin = columns[2].get_text(strip=True)
                airline = columns[3].get_text(strip=True)
                aircraft = columns[4].get_text(strip=True)
                status = columns[6].get_text(strip=True)
                status_div = row.find('div', class_='state-block')
                status_color = status_div.get('class')[1] if status_div else 'unknown'
                data_date = row.get('data-date')
                first_date_obj = datetime.strptime(data_date, '%A, %b %d').replace(year=datetime.now().year)
                first_date_str = first_date_obj.strftime('%Y-%m-%d')

                
                flights.append({
                    'Time': time,
                    'Flight': flight,
                    'From': origin,
                    'Airline': airline,
                    'Aircraft': aircraft,
                    'Status': status,
                    'Delay_status':status_color,
                    'date_flight':first_date_str
                })
    voos = pd.DataFrame(flights)

    return voos



voos_chegada = obter_voos(arrivals_url)

time.sleep(10)

voos_partida = obter_voos(departures_url)

data_hoje = datetime.today()
data_ontem = data_hoje - timedelta(days=1)
data_filtro = data_ontem.strftime('%Y-%m-%d')

voos_partida['direcao'] = 'embarque'
voos_chegada['direcao'] = 'desembarque'

voos = pd.concat([voos_partida, voos_chegada], ignore_index=True)

voos[['From', 'Aeroporto']] = voos['From'].str.extract(r'(.+)\((.+)\)-')

voos['Airline'] = voos['Airline'].str.replace(r'\s*\(.*?\)-', '', regex=True)
voos['Airline'] = voos['Airline'].str.replace(r'\-$', '', regex=True)

voos[['Aircraft', 'Aircraft_type']] = voos['Aircraft'].str.extract(r'(.+)\((.+)\)')

voos[['Status', 'Hora_realizada']] = voos['Status'].str.extract(r'([a-zA-Z]+)(\d{2}:\d{2})?')

url = "https://simplemaps.com/static/data/world-cities/basic/simplemaps_worldcities_basicv1.75.zip"


response = requests.get(url)
zip_file = zipfile.ZipFile(BytesIO(response.content))


with zip_file.open('worldcities.csv') as file:
    df = pd.read_csv(file)


df['city_normalized'] = df['city'].apply(lambda x: unidecode(str(x)))


def obter_informacoes_geograficas(cidade):
    resultado = df[df['city_normalized'] == cidade][['city','admin_name', 'country']].values
    if len(resultado) > 0:
        cidade, estado, pais = resultado[0]
        return cidade, estado, pais
    else:
        return None, None, None

voos[['Cidade_Correta' ,'Estado/Província', 'País']] = voos['From'].apply(lambda x: pd.Series(obter_informacoes_geograficas(x)))

def obter_nacionalidade(row):
    if row != 'Brazil':
        return 'Internacional'
    return 'Nacional'

voos['Is_National'] = voos['País'].apply(obter_nacionalidade)


colunas_traduzidas = {
    'Time': 'Hora_Prevista',
    'Flight': 'Voo',
    'From': 'Origem',
    'Airline': 'Companhia_Aerea',
    'Aircraft': 'Aeronave',
    'Status': 'Status',
    'Delay_status': 'Status_Atraso',
    'date_flight': 'Data_Voo',
    'direcao': 'Direcao',
    'Aeroporto': 'Aeroporto',
    'Aircraft_type': 'Tipo_Aeronave',
    'Hora_realizada': 'Hora_Realizada',
    'Estado/Província': 'Estado_Provincia',
    'País': 'Pais',
    'Is_National': 'Tipo_Voo',
    'Cidade_Correta': 'Cidade_Normalizada'
}


voos = voos.rename(columns=colunas_traduzidas)

def obter_atraso_flag(row):
    if pd.isna(row['Hora_Prevista']) or pd.isna(row['Hora_Realizada']):
        return row['Hora_Realizada']
    else:
        hora_prevista = pd.to_datetime(row['Hora_Prevista'], format='%H:%M').strftime('%I:%M %p')
        hora_realizada = pd.to_datetime(row['Hora_Realizada'], format='%H:%M').strftime('%I:%M %p')

        if hora_realizada >= hora_prevista:
            return 'Atrasado'
        else:
            return 'ON-Time'




 def obter_atraso_tempo(row):
    
    if pd.isna(row['Hora_Prevista']) or pd.isna(row['Hora_Realizada']):
        return row['Hora_Realizada']
    else:
        
        hora_prevista = pd.to_datetime(row['Hora_Prevista'], format='%H:%M').strftime('%I:%M %p')
        hora_realizada = pd.to_datetime(row['Hora_Realizada'], format='%H:%M').strftime('%I:%M %p')

        hora_prevista_calc = pd.to_datetime(row['Hora_Prevista'], format='%H:%M')
        hora_realizada_calc = pd.to_datetime(row['Hora_Realizada'], format='%H:%M')



        if hora_realizada > hora_prevista:
            atraso = hora_realizada_calc - hora_prevista_calc
            horas = atraso.seconds // 3600
            minutos = (atraso.seconds % 3600) // 60
            return f"{horas:02}:{minutos:02}"
        else:
            atraso = hora_prevista_calc - hora_realizada_calc
            horas = atraso.seconds // 3600
            minutos = (atraso.seconds % 3600) // 60
            return f"{horas:02}:{minutos:02}"



voos['Flag'] = voos.apply(obter_atraso_flag,axis=1)

voos['Atraso\Antecipado'] = voos.apply(obter_atraso_tempo,axis=1)


def obter_status_real(row):
    if row['Status'] == 'Canceled':
        return row['Status']
    elif row['Status'] == 'Diverted':
        return row['Status']
    elif row['Status_Atraso'] == 'red' and not (row['Status'] == 'Canceled' or row['Status'] == 'Diverted'):
        return 'Delayed'
    elif row['Status_Atraso'] == 'yellow':
        return 'Delayed'
    elif row['Status_Atraso'] == 'gray' or row['Status']=='Estimated':
        return 'Unknown'
    return 'ON-TIME'


voos['Voo_Status_Real'] = voos.apply(obter_status_real,axis=1)


credentials = (
    'Driver={ODBC Driver 17 for SQL Server};'
    f'Server={os.environ["AZURE_SQL_SERVER"]};'
    f'Database={os.environ["AZURE_SQL_DATABASE"]};'
    f'Uid={os.environ["AZURE_SQL_USER"]};'
    f'pwd={os.environ["AZURE_SQL_PASSWORD"]}'
)



max_retries = 3
attempt = 0
connected = False

while attempt < max_retries and not connected:
    try:
        conn = pyodbc.connect(credentials,timeout=40)		
        connected = True
    except pyodbc.Error as e:
        print(f"Connection attempt {attempt + 1} failed: {e}")
        attempt += 1
        time.sleep(10)

cursor = conn.cursor()

conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
conn.setencoding('latin1')

voos = voos.fillna('')

insert_to_flights_stmt = '''
INSERT INTO [dbo].[Voos] (
     [Hora_Prevista], [Voo], [Origem], [Companhia_Aerea], [Aeronave], [Status], [Status_Atraso], [Data_Voo],
     [Direcao], [Aeroporto], [Tipo_Aeronave], [Hora_Realizada], [Cidade_Normalizada], [Estado_Provincia],
    [Pais], [Tipo_Voo], [Flag], [Atraso/Antecipado], [Voo_Status_Real]
) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''


cursor.executemany(insert_to_flights_stmt, voos.values.tolist()) 


print(f'{len(voos)} rows inserted in pd_df_california_housing table')
           

cursor.commit()        
cursor.close()
conn.close()