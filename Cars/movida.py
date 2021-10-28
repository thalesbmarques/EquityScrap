import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import os
import logging
import sqlite3
import yaml
import itertools
import sys
from timeit import default_timer as timer

# Adjusting CWD (Just for testing)
os.chdir('/Users/thalesmarques/PycharmProjects/EquityScrap')

# Log Config
logging.basicConfig(level=logging.DEBUG, filename='data.log', filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
stdout_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stdout_handler)

# Todo: Warn if something is missing (probably on clean_movida_html)
# Todo: Retry if something is missing (how?)

start = timer()

def get_params():
    """
    Get manual parameters from movida_requests
    Returns
    -------
    dict
        params
    """
    # Inputs
    with open('Cars/movida_request.yaml') as file:
        # The FullLoader parameter handles the conversion from YAML
        # scalar values to Python the dictionary format
        params = yaml.load(file, Loader=yaml.FullLoader)

    return params


def get_mass_params():
    """
    Read movida_mass_requests.yaml and process the inputs for the function
    Returns
    -------
    dictionary
        params
    """
    # Opening yaml file
    with open('Cars/movida_mass_request.yaml') as file:
        # The FullLoader parameter handles the conversion from YAML
        # scalar values to Python the dictionary format
        params_mass = yaml.load(file, Loader=yaml.FullLoader)

    # Creating all combinations possible
    today = datetime.today().date()
    date_range = params_mass['date_range']
    params_mass['dates'] = []
    params_mass['end_date'] = []
    params_mass['start_time'] = []

    # Getting Combinations of start_date
    for dt in params_mass['date_interval']['start_intervals']:
        for i in range(dt, date_range + dt):
            for dt_end in params_mass['date_interval']['end_intervals']:
                params_mass['dates'].append((today + timedelta(days=i), today + timedelta(days=i + dt_end)))

    # Removing duplicates
    params_mass['dates'] = list(set(params_mass['dates']))

    # Creating all possible combinations
    params_comb = list(itertools.product(params_mass['dates'], params_mass['time'], params_mass['places']))

    # Creating output params
    params = dict()
    params['start_dt'] = [x[0][0].strftime('%d/%m/%Y') for x in params_comb]
    params['end_dt'] = [x[0][1].strftime('%d/%m/%Y') for x in params_comb]
    params['start_time'] = [x[1] for x in params_comb]
    params['end_time'] = [x[1] for x in params_comb]
    params['place'] = [x[2] for x in params_comb]

    return params


def _scrap_movida(start_dt, end_dt, start_time, end_time, place):
    """
    Scrap https://www.movida.com.br
    Parameters
    ----------
    start_dt:  str
        Rent start date, format dd/mm/yyyy
    end_dt:  str
        Rent end date, format dd/mm/yyyy
    start_time:  str
        Rent start time, format hh:mm
    end_time:  str
        Rent end time, format hh:mm
    place:  str
        Place to pick up and delivery - Using movida code available on txt
    Returns
    -------
    bs4.BeautifulSoup
        Beautiful Soup HTML Object
    """

    # Connecting to webdriver
    movida = 'https://www.movida.com.br'
    options = webdriver.ChromeOptions()
    service = Service('webdriver/chromedriver')

    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("--disable-extensions")
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # options.add_argument("--headless")  # not working
    driver = webdriver.Chrome(service=service, options=options)
    # driver = webdriver.Safari()  # only for tests

    logger.info(
        "[DRIVER STARTED] Place:%s, Start:%s - %s, End:%s - %s, " % (place, start_dt, start_time, end_dt, end_time))

    logger.info(
        "[REQUESTING HOMEPAGE] Place:%s, Start:%s - %s, End:%s - %s, " % (
            place, start_dt, start_time, end_dt, end_time))

    # Opening Website
    driver.get(movida)

    # Filling place
    driver.implicitly_wait(20)
    element_iata = driver.find_element(by='xpath', value='//*[@id="formSearchEngine"]/div/div[1]/input[2]')

    # Date Elements
    element_dt_ret = driver.find_element(by='xpath', value='//*[@id="data_retirada"]')
    element_tm_ret = driver.find_element(by='xpath', value='//*[@id="hora_retirada"]')

    element_dt_dev = driver.find_element(by='xpath', value='//*[@id="data_devolucao"]')
    element_tm_dev = driver.find_element(by='xpath', value='//*[@id="hora_devolucao"]')

    logger.info(
        "[REQUESTING DATA] Place:%s, Start:%s - %s, End:%s - %s, " % (place, start_dt, start_time, end_dt, end_time))

    # Search Button
    element_search_bt = driver.find_element(by='xpath', value='//*[@id="formSearchEngine"]/div/button')

    # Filling elements
    driver.execute_script('''
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    ''', element_iata, place)

    driver.execute_script('''
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    ''', element_dt_ret, start_dt)

    driver.execute_script('''
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    ''', element_tm_ret, start_time)

    driver.execute_script('''
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    ''', element_dt_dev, end_dt)

    driver.execute_script('''
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    ''', element_tm_dev, end_time)

    # Clicking on search
    element_search_bt.click()

    # Storing HTML
    time.sleep(15)
    html = BeautifulSoup(driver.page_source, features="html.parser")

    logger.info(
        "[DRIVER CLOSED] Place:%s, Start:%s - %s, End:%s - %s, " % (place, start_dt, start_time, end_dt, end_time))

    # Getting Data
    driver.close()

    logger.info(
        "[SCRAP PASSED] Place:%s, Start:%s - %s, End:%s - %s, " % (place, start_dt, start_time, end_dt, end_time))

    return html


def _clean_movida_html(html):
    """
    Clean HTML Object scraped from Movida website
    Parameters
    ----------
    html:  bs4.BeautifulSoup
        html retrieved from scrap_movida function
    Returns
    -------
    tuple
        tuple of lists containing groups, cars and prices
    """

    groups = []
    cars = []
    prices = []
    # Block car contains all information to each group
    block_car = html.findAll("div", {"class": "block-car"})

    for block in block_car:
        try:
            # Getting Groups' names
            groups.append(block.find("div", {"class": "col-lg-12 title-group_walk"}).text)
            # Getting Groups' cars
            cars.append(
                block.find("div", {"class": "veiculoBox__container"}).find("div",
                                                                           {"class": "text-transform--initial"}).text)
            # Getting Group's prices
            prices.append(block.find("span", {
                "class": ["clube-price__value-discount--size_walk"]}).text)
        except AttributeError:
            pass

    # Formatting data
    prices = list(map(lambda s: s.strip(), prices))
    prices = list(map(lambda s: s.replace('.', ''), prices))
    prices = list(map(lambda s: s.replace(',', '.'), prices))
    prices = list(map(lambda s: float(s), prices))

    cars = list(map(lambda s: s.replace('\n', ' '), cars))

    return groups, cars, prices


def scrap_movida(start_dt, end_dt, start_time, end_time, place):
    """
    Scrap and process movida data (https://www.movida.com.br)
    Parameters
    ----------
    start_dt:  str
        Rent start date, format dd/mm/yyyy
    end_dt:  str
        Rent end date, format dd/mm/yyyy
    start_time:  str
        Rent start time, format hh:mm
    end_time:  str
        Rent end time, format hh:mm
    place:  str
        Place to pick up and delivery - Using movida code available on txt

    Returns
    -------
    pandas.DataFrame
        Prices dataframe


    """
    html_mov = _scrap_movida(start_dt, end_dt, start_time, end_time, place)

    logger.info(
        "[CLEANING HTML] Cleaning html object and creating Data Frame Place:%s, Start:%s - %s, End:%s - %s, " % (
            place, start_dt, start_time, end_dt, end_time))

    groups, cars, prices = _clean_movida_html(html_mov)

    logger.info("[CLEANING PASSED] Cleaning Passed Place:%s, Start:%s - %s, End:%s - %s, " % (
        place, start_dt, start_time, end_dt, end_time))

    df_out = pd.DataFrame()

    logger.info("[CREATING DATAFRAME] Cleaning Passed Place:%s, Start:%s - %s, End:%s - %s, " % (
        place, start_dt, start_time, end_dt, end_time))

    # Final Dataframe
    df_out['place'] = np.repeat(place, len(groups))
    df_out['extraction_dt'] = datetime.now()
    df_out['car_group'] = groups
    df_out['start_date'] = start_dt
    df_out['end_date'] = end_dt
    df_out['start_time'] = start_time
    df_out['end_time'] = end_time
    df_out['cars'] = cars
    df_out['prices'] = prices

    logger.info("[DATAFRAME PASSED] Cleaning Passed Place:%s, Start:%s - %s, End:%s - %s, " % (
        place, start_dt, start_time, end_dt, end_time))

    return df_out


def set_up_threads(params):
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(scrap_movida,
                               params['start_dt'],
                               params['end_dt'],
                               params['start_time'],
                               params['end_time'],
                               params['place'],
                               timeout=None)
        finals = []
        for value in results:
            finals.append(value)

        return finals


if __name__ == '__main__':
    params_ = get_mass_params()
    df_list = set_up_threads(params_)
    df = pd.concat(df_list)
    # Connecting to sql
    conn = sqlite3.connect('db/data.db3')
    # Exporting to sql
    df.to_sql('movida', con=conn, if_exists='append', index=False)
    # Closing connection
    conn.close()
    end = timer()
    print(timedelta(seconds=end - start))
