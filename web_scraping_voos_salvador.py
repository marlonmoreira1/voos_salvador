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
import os
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = Options()
options.add_argument('--headless')  
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

arrivals_url = 'https://www.flightradar24.com/data/airports/ssa/arrivals'
departures_url = 'https://www.flightradar24.com/data/airports/ssa/departures'


def fechar_overlay():
    try:        
        overlay = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "onetrust-pc-dark-filter"))
        )
        fechar_botao = driver.find_element(By.ID, "onetrust-accept-btn-handler")
        fechar_botao.click()
    except Exception as e:
        print("Overlay não encontrado ou erro ao fechá-lo:", e)


def obter_voos(url):
    import time
    url = url
    driver.get(url)

    fechar_overlay()

    while True:
        try:
            load_more_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@class='btn btn-table-action btn-flights-load']")))
                    
            load_more_button.click()
            time.sleep(5)
        except:
            break
            
    time.sleep(5)
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
                    'Delay_status': status_color,
                    'date_flight': first_date_str
                })
    voos = pd.DataFrame(flights)

    return voos



voos_chegada = obter_voos(arrivals_url)

time.sleep(10)

voos_partida = obter_voos(departures_url)

data_hoje = datetime.today()
data_ontem = data_hoje - timedelta(days=1)
data_filtro = data_ontem.strftime('%Y-%m-%d')

voos_partida = voos_partida[voos_partida['date_flight']==data_filtro]
voos_chegada = voos_chegada[voos_chegada['date_flight']==data_filtro]

voos_partida['direcao'] = 'embarque'
voos_chegada['direcao'] = 'desembarque'

voos = pd.concat([voos_partida, voos_chegada], ignore_index=True)

voos[['From', 'Aeroporto']] = voos['From'].str.extract(r'(.+)\((.+)\)-')

voos['Airline'] = voos['Airline'].str.replace(r'\s*\(.*?\)-', '', regex=True)
voos['Airline'] = voos['Airline'].str.replace(r'\-$', '', regex=True)

voos[['Aircraft', 'Aircraft_type']] = voos['Aircraft'].str.extract(r'(.+)\((.+)\)')

voos[['Status', 'Hora_realizada','AM-PM_Realizado']] = voos['Status'].str.extract(r'([a-zA-Z\s\.]+)(\d{1,2}:\d{2})?\s?(AM|PM)?')

voos[['Time', 'AM-PM_Previsto']] = voos['Time'].str.extract(r'(\d{1,2}:\d{2})\s?(AM|PM)')

def converter_data(data_str):
    """Converte uma data no formato YYYY-MM-DD para o formato 'ddd, d mmm'"""
    data_obj = datetime.strptime(data_str, "%Y-%m-%d")
    dias_semana = ["seg", "ter", "qua", "qui", "sex", "sab","dom"]
    meses = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
    
    dia_semana = dias_semana[data_obj.weekday()]
    dia = data_obj.day
    mes = meses[data_obj.month - 1]
    
    return f"{dia_semana}, {dia} {mes}"


def buscar_horario_chegada(numero_voo,data_desejada):
    base_url = "https://br.trip.com/flights/status-"
    url = base_url + numero_voo      
    driver.get(url)
    
    time.sleep(5)  

    try:
        
        data_formatada = converter_data(data_desejada)        
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')        
        
        tabela = soup.find('table')
        
        if tabela:
            
            linhas = tabela.find_all('tr')            
            
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) > 6:
                    data_linha = colunas[1].text.strip() 
                    chegada = colunas[7].text.strip()
                    status = colunas[8].text.strip()
                    if status == 'Cancelado':
                        return status
                    else:
                        if data_formatada == data_linha:
                            return chegada
        else:
            return "Tabela não encontrada"
    except Exception as e:
        print(f"Erro ao buscar dados: {e}")
        return "Erro"


def atualizar_hora(row):
    if row['Status'] == 'Unknown':             
        
        horario = buscar_horario_chegada(row['Flight'],row['date_flight'])
        
        if horario == 'Cancelado':
            return row['Hora_realizada']
        elif horario == 'Tabela não encontrada' or horario == '--:--':
            return row['Hora_realizada']
        return horario        
        
    return row['Hora_realizada']


def atualizar_status(row):
    if row['Status'] == 'Unknown':
        
        status = buscar_horario_chegada(row['Flight'],row['date_flight'])
        
        if status == 'Cancelado':
            return 'Canceled'
        elif status == 'Tabela não encontrada' or status == '--:--':
            return row['Status']
        return 'Known'            
        
    return row['Status']


def am_pm_realizado(row):

    if row['Status'] == 'Known':
        hora_realizada = pd.to_datetime(row['Hora_realizada'])
        if hora_realizada.hour > 12:
            return 'PM'
        else:
            return 'AM'
    return row['AM-PM_Realizado']

voos['Hora_realizada'] = voos.apply(atualizar_hora, axis=1)

voos['Status'] = voos.apply(atualizar_status, axis=1)

voos['AM-PM_Realizado'] = voos.apply(am_pm_realizado, axis=1)

url = "https://simplemaps.com/static/data/world-cities/basic/simplemaps_worldcities_basicv1.75.zip"

response = requests.get(url)
zip_file = zipfile.ZipFile(BytesIO(response.content))


with zip_file.open('worldcities.csv') as file:
    df = pd.read_csv(file)


df['city_normalized'] = df['city'].apply(lambda x: unidecode(str(x)))


def obter_informacoes_geograficas(cidade):
    resultado = df[df['city_normalized'].str.lower() == cidade.lower()][['city','admin_name', 'country']].values
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
    'Cidade_Correta': 'Cidade_Normalizada',
    'AM-PM_Previsto':'AM-PM_Previsto',
    'AM-PM_Realizado':'AM-PM_Realizado'
}


voos = voos.rename(columns=colunas_traduzidas)

def is_null(row):
    if pd.isna(row['Hora_Prevista']) or pd.isna(row['Hora_Realizada']): 
        return row['Hora_Realizada']
    return None


def convert_to_24h(time_str, am_pm,status,tipo):
    if status == 'Known' and tipo == 'realizado':
        time_obj = datetime.strptime(time_str, '%H:%M')
        return time_obj
    time_obj = datetime.strptime(time_str, '%I:%M')
    if am_pm == 'PM' and time_obj.hour != 12:
        time_obj += timedelta(hours=12)
    elif am_pm == 'AM' and time_obj.hour == 12:
        time_obj -= timedelta(hours=12)
    return time_obj
    

def obter_atraso_flag(row):
    
    result = is_null(row)

    if result is not None:
        return result
    
    hora_prevista = convert_to_24h(row['Hora_Prevista'], row['AM-PM_Previsto'],row['Status'],'previsto')
    hora_realizada = convert_to_24h(row['Hora_Realizada'], row['AM-PM_Realizado'],row['Status'],'realizado')       

    if hora_realizada > hora_prevista:
        return 'Atrasado'
    else:
        return 'ON-Time'
   

def obter_diff(hora_prevista,hora_realizada,am_pm_previsto,am_pm_realizado):
    
    if hora_prevista.hour == 0 and (am_pm_previsto == 'AM' and am_pm_realizado == 'PM'):
        atraso = hora_prevista - hora_realizada
    elif hora_prevista.hour == 12 and (am_pm_previsto == 'PM' and am_pm_realizado == 'AM'):
        atraso = hora_prevista - hora_realizada
    elif hora_prevista > hora_realizada and (am_pm_previsto == am_pm_realizado):
        atraso = hora_prevista - hora_realizada
    else:
        atraso = hora_realizada - hora_prevista    
    
    if atraso < timedelta(0):
        atraso += timedelta(days=1)
    return atraso


def obter_atraso_tempo(row):
    
    result = is_null(row)

    if result is not None:
        return result
    
    hora_prevista = convert_to_24h(row['Hora_Prevista'], row['AM-PM_Previsto'],row['Status'],'previsto')
    hora_realizada = convert_to_24h(row['Hora_Realizada'], row['AM-PM_Realizado'],row['Status'],'realizado')
    
    atraso = obter_diff(hora_prevista,hora_realizada,row['AM-PM_Previsto'],row['AM-PM_Realizado'])
    
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
    elif (row['Status_Atraso'] == 'red' and not (row['Status'] == 'Canceled' or row['Status'] == 'Diverted'))\
    or (row['Status_Atraso'] == 'yellow' and pd.to_datetime(row['Atraso\Antecipado']) > pd.to_datetime('00:15'))\
    or (row['Flag'] == 'Atrasado' and pd.to_datetime(row['Atraso\Antecipado']) > pd.to_datetime('00:15')):
        return 'Delayed'
    elif row['Status_Atraso'] == 'gray'and not row['Status'] == 'Known':
        return 'Unknown'
    return 'ON-TIME'


voos['Voo_Status_Real'] = voos.apply(obter_status_real,axis=1)
gol = voos[voos['Companhia_Aerea']=='GOL Linhas Aereas']
print(gol[['Hora_Prevista','Hora_Realizada','Voo_Status_Real','Atraso\Antecipado']].head(55))
mudado = voos[voos['Status']=='Known']
print(mudado[['Hora_Prevista','Hora_Realizada','Voo_Status_Real','Atraso\Antecipado']].head(55))
# credentials = (
#     'Driver={ODBC Driver 17 for SQL Server};'
#     f'Server={os.environ["AZURE_SQL_SERVER"]};'
#     f'Database={os.environ["AZURE_SQL_DATABASE"]};'
#     f'Uid={os.environ["AZURE_SQL_USER"]};'
#     f'pwd={os.environ["AZURE_SQL_PASSWORD"]}'
# )


# max_retries = 3
# attempt = 0
# connected = False

# while attempt < max_retries and not connected:
#     try:
#         conn = pyodbc.connect(credentials,timeout=20)		
#         connected = True
#     except pyodbc.Error as e:
#         print(f"Connection attempt {attempt + 1} failed: {e}")
#         attempt += 1
#         time.sleep(10)

# cursor = conn.cursor()

# voos = voos.fillna('')

# insert_to_flights_stmt = '''
# INSERT INTO [dbo].[Voos] (
#      [Hora_Prevista], [Voo], [Origem], [Companhia_Aerea], [Aeronave], [Status], [Status_Atraso], [Data_Voo],
#      [Direcao], [Aeroporto], [Tipo_Aeronave], [Hora_Realizada], [AM-PM], [Cidade_Normalizada], [Estado_Provincia],
#     [Pais], [Tipo_Voo], [Flag], [Atraso/Antecipado], [Voo_Status_Real]
# ) 
# VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
# '''

# nova_ordem = ['Hora_Prevista', 'Voo', 'Origem', 'Companhia_Aerea', 'Aeronave', 'Status', 'Status_Atraso', 'Data_Voo',
#      'Direcao', 'Aeroporto', 'Tipo_Aeronave', 'Hora_Realizada', 'AM-PM_Previsto', 'Cidade_Normalizada', 'Estado_Provincia',
#     'Pais', 'Tipo_Voo', 'Flag', 'Atraso\Antecipado', 'Voo_Status_Real']

# voos = voos.drop('AM-PM_Realizado', axis=1)

# voos = voos[nova_ordem]

# cursor.executemany(insert_to_flights_stmt, voos.values.tolist()) 


# print(f'{len(voos)} rows inserted in Voos table')
           

# cursor.commit()        
# cursor.close()
# conn.close()
