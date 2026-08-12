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


# logger = logging.getLogger("BatchLogger")
# file_log = logging.getLogger("file_logger")
# db_logger_name=""


def get_loop_factory():
    # На Windows принудительно включаем SelectorEventLoop
    if sys.platform == "win32":
        return lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    # На Linux/macOS возвращаем None (Python сам выберет epoll/kqueue)
    return None

# MAIN 
# ===============================================================================

async def main():
    # Инициализация логгера 
    d_logger = AsyncSplitLogger(db_pool=None, file_path="./log/fatalerror.log", db_logger_name="", batch_size=10)  # BatchLogger
    d_logger.start()

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
        d_logger.error_file(f"Критическая ошибка DatabaseManager: {err}")
        await d_logger.stop()  # Корректно завершаем воркер перед выходом
        sys.exit(1)
    except Exception as err:
        print(f"db_manager: {err}", flush=True)
        d_logger.error_file(f"Непредвиденная ошибка DatabaseManager: {err}")
        await d_logger.stop()
        sys.exit(1)

    try:
        d_logger.info_db("Инициализация пула соединений PostgreSQL...")
        await db_mgr.initialize_pool(db_config)
        # передаём пул в логгер
        d_logger.set_pool(db_mgr.pool)
        d_logger.info_db("Пул соединений успешно инициализирован. Логгер переведен в штатный режим.")
    except Exception as err:
        d_logger.error_file(f"Ошибка initialize_pool: {err}")
        await d_logger.stop()
        sys.exit(1)

#    file_log.info(f"test2: {db_config}")

    # Основной блок 
    d_logger.info_db(f"{'начало работы скрипта':=^40}")
    # проверим наличие данных в таблице, если их нет то надо заполнить
    result = await db_mgr.check_table_data()
    if result:
        last_date = result[0]
    else:
        last_date = dt.date(1970, 1, 1)
    curdate = dt.datetime.now()
    dt_delta = dt.timedelta(days=1)
    df_list = []
    err_cnt = 0
    if  last_date == dt.datetime(1970, 1, 1).date(): 
        d_logger.info_db('Начальное заполнение данными')
        filldata = True
        while filldata:
            cur_df = last_data(api_url, curdate)
            if isinstance(cur_df, pd.DataFrame):
                d_logger.info_db(f'Обработка данных за {curdate.strftime('%Y-%m-%d')}')
                err_cnt = 0
                if cur_df.empty:
                    d_logger.info_db(f'Нет данных для записи в БД за {curdate.strftime('%Y-%m-%d')}')
                else:
                    d_logger.info_db(f'начало записи данных в БД за {curdate.strftime('%Y-%m-%d')}')
                    await db_mgr.write_data(cur_df)
            else:    
                if cur_df == 'Информация за более ранние периоды отсутствует':
                    filldata = False
                else: # считаем ошибки
                    err_cnt += 1
                    if err_cnt > 10:    # выходим если ошибок больше 10
                        filldata = False

            curdate -= dt_delta    
    else:
        # заполняем данными за прошлый день
        d_logger.info_db('Регулярное заполнение данными')
        d_range = (curdate.date() - last_date).days + 1
        x_date = last_date + dt_delta # день выгрузки данных на last_date
        for i in range(2, d_range):
            x_date += dt_delta # день выгрузки данных за вчера
            d_logger.info_db(f"Начало чтения данных по API за {(x_date - dt_delta).strftime('%Y-%m-%d')} ")
            cur_df = last_data(api_url, x_date)
            if isinstance(cur_df, pd.DataFrame):
                if cur_df.empty:
                    d_logger.info_db(f'Нет данных для записи в БД за {(x_date - dt_delta).strftime('%Y-%m-%d')}')
                else:
                    d_logger.info_db(f'начало записи данных в БД за {(x_date - dt_delta).strftime('%Y-%m-%d')}')
                    await db_mgr.write_data(cur_df)

    # проверка пропущенных дат
    lost_dates = await db_mgr.missing_dates('2022-01-01')

    if lost_dates and isinstance(lost_dates, list):
        d_logger.info_db('Заполнение пропущенных дат')
        for date in result:
            x_date = date + dt_delta
            d_logger.info_db(f"Начало чтения данных по API за {dt.strftime('%Y-%m-%d')} ")
            cur_df = last_data(api_url, x_date)
            if isinstance(cur_df, pd.DataFrame):
                if cur_df.empty:
                    d_logger.info_db(f'Нет данных для записи в БД за {(x_date - dt_delta).strftime('%Y-%m-%d')}')
                else:
                    d_logger.info_db(f'начало записи данных в БД за {(x_date - dt_delta).strftime('%Y-%m-%d')}')
                    await db_mgr.write_data(cur_df)


    # очистка старых логов
    cln_date = (dt.datetime.now() - cln_days * dt_delta).strftime('%Y-%m-%d')
    await db_mgr.log_cleanup(cln_date)

    d_logger.info_db(f"{' Завершение работы скрипта ':=^40}")
    d_logger.info_db(f"{'====&&&&&&&&&&&&&&&&&&&====':=^40}")
    
    # Даем время логам записаться перед выходом
    await asyncio.sleep(1)
    # Завершение работы логгера
    await d_logger.stop()

    db_instance = DatabaseManager(db_config)
    if db_instance:
        await db_instance.close_all()


if __name__ == "__main__":
#    asyncio.run(main(), loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
    asyncio.run(main(), loop_factory=get_loop_factory())
