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
    
    load_more_button = driver.find_element(By.XPATH, "//button[@class='btn btn-table-action btn-flights-load']")
    for _ in range(2):
        load_more_button.click()
        time.sleep(25)
    time.sleep(25)
    element = WebDriverWait(driver, 30).until(
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

time.sleep(30)

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

voos[['Status', 'Hora_realizada']] = voos['Status'].str.extract(r'([a-zA-Z]+)(\d{1,2}:\d{2})?')

voos['Time'] = voos['Time'].str.extract(r'(\d{1,2}:\d{2})')

print(voos_partida[['Status','Time']].head(55))
