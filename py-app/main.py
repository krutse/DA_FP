from ecom import *
import asyncio
import selectors
# from ecom import DatabaseManager
import queue, logging, time, sys
import pandas as pd
import datetime as dt
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg


logger = logging.getLogger("BatchLogger")
file_log = logging.getLogger("file_logger")

# MAIN 
# ===============================================================================

async def main():
    # считываем переменные
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)

    db_config = {   
            'host': os.environ.get('POSTGRES_HOST', 'localhost'),   # POSTGRES_HOST           # DB_HOST
            'port': int(os.environ.get('POSTGRES_PORT', 5432)),     # POSTGRES_PORT           # DB_PORT
            'dbname': os.environ.get('APP_DB', 'ecom'),             # APP_DB  'database'      # DB_NAME
            'user': os.environ.get('APP_USER', 'tst_user1'),        # APP_USER                # DB_USER
            'password': os.environ.get('APP_USER_PASSWORD', '111')  # APP_USER_PASSWORD       # DB_PASSWORD
        }

    log_level = os.environ.get('LOG_LEVEL', 'logging.INFO')
    api_url = os.environ.get('API_URL', 'http://final-project.simulative.ru/data')
    cln_days = int(os.environ.get('LOG_DAYS', 30))

#    print("CURRENT DB CONFIG: ", db_config, flush=True)

    # Инициализируем Singleton DatabaseManager
    try:
        db_mgr = DatabaseManager(db_config=db_config)
    except psycopg.OperationalError  as err:
        # pass # write fatal error log
        print(f"db_manager: {err}", flush=True)
        sys.exit(1)
    except Exception as err:
        print(f"db_manager: {err}", flush=True)
        # pass
        sys.exit(1)

    await db_mgr.initialize_pool(db_config)

    # Инициализация логгера 
    d_logger = AsyncSplitLogger(db_pool=db_mgr.pool, file_path="./log/fatalerror.log", db_logger_name="BatchLogger", batch_size=10)
    d_logger.start()

#    file_log.info(f"test2: {db_config}")

    # Основной блок 
    logger.info(f"{'начало работы скрипта':=^40}")
    # проверим наличие данных в таблице, если их нет то надо заполнить
    result = await db_mgr.check_table_data()
    if result:
        last_date = result[0]
    else:
        last_date = dt.date(1970, 1, 1)
    curdate = dt.datetime.now()
    dt_delta = dt.timedelta(days=1)
    df_list = []
    if  last_date == dt.datetime(1970, 1, 1).date(): 
        logger.info('Начальное заполнение данными')
        filldata = True
        while filldata:
            cur_df = last_data(api_url, curdate)
            if isinstance(cur_df, pd.DataFrame):
                logger.info(f'Обработка данных за {curdate.strftime('%Y-%m-%d')}')
                if cur_df.empty:
                    logger.info(f'Нет данных для записи в БД за {curdate.strftime('%Y-%m-%d')}')
                else:
                    logger.info(f'начало записи данных в БД за {curdate.strftime('%Y-%m-%d')}')
                    await db_mgr.write_data(cur_df)
            else:    
                filldata = False
            curdate -= dt_delta    
    else:
        # заполняем данными за прошлый день
        logger.info('Регулярное заполнение данными')
        d_range = (curdate.date() - last_date).days + 1
        x_date = last_date + dt_delta # день выгрузки данных на last_date
        for i in range(2, d_range):
            x_date += dt_delta # день выгрузки данных за вчера
            logger.info(f"Начало чтения данных по API за {(x_date - dt_delta).strftime('%Y-%m-%d')} ")
            cur_df = last_data(api_url, x_date)
            if isinstance(cur_df, pd.DataFrame):
                if cur_df.empty:
                    logger.info(f'Нет данных для записи в БД за {(x_date - dt_delta).strftime('%Y-%m-%d')}')
                else:
                    logger.info(f'начало записи данных в БД за {(x_date - dt_delta).strftime('%Y-%m-%d')}')
                    await db_mgr.write_data(cur_df)

    # очистка старых логов
    cln_date = (dt.datetime.now() - cln_days * dt_delta).strftime('%Y-%m-%d')
    await db_mgr.log_cleanup(cln_date)

    logger.info(f"{' Завершение работы скрипта ':=^40}")
    logger.info('====&&&&&&&&&&&&&&&&&&&====')
    
    # Даем время логам записаться перед выходом
    await asyncio.sleep(1)
    # Завершение работы логгера
    await d_logger.stop()

    db_instance = DatabaseManager(db_config)
    if db_instance:
        await db_instance.close_all()


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))

