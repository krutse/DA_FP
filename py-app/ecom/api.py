from .log import *
import requests, json
import pandas as pd


# get raw api data
# запрос выдает данные за дату в params.
def get_data(url,params):
    try:
        rsp = requests.get(url,params=params, timeout=10)
        rsp.raise_for_status() 
        return rsp #.json()
    except requests.exceptions.HTTPError as err:
        logger.error(f"Произошла HTTP ошибка: {err}")    
    except Exception as err:
        logger.error(f"Другая ошибка: {err}")    

# выгрузка данных в dataframe
def last_data(url, today = dt.datetime.now() ):
    """ функция возвращает данные за предыдущий день"""
    yesterday = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    params = {'date': yesterday}
    rsp = get_data(url, params)
    if rsp:
        if not rsp.text == 'Информация за более ранние периоды отсутствует':
            df = pd.json_normalize(rsp.json())
            return df
        else:
            return False
    else:
        logger.error("Ошибка получения данных")      