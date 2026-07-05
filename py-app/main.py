from ecom import *
# from ecom import DatabaseManager
import queue, logging, time, sys
import pandas as pd
import datetime as dt
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2


# MAIN 
# ===============================================================================
# считываем переменные
def main():
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)

    db_config = {   
            'host': os.environ.get('DB_HOST', 'localhost'),   # POSTGRES_HOST
            'port': int(os.environ.get('DB_PORT', 5432)),     # POSTGRES_PORT
            'database': os.environ.get('DB_NAME', 'ecom'),           # APP_DB
            'user': os.environ.get('DB_USER', 'tst_user1'),        # APP_USER
            'password': os.environ.get('DB_PASSWORD', '111')  # APP_USER_PASSWORD
        }

    log_level = os.environ.get('LOG_LEVEL', 'logging.INFO')
    api_url = os.environ.get('API_URL', 'http://final-project.simulative.ru/data')
    cln_days = int(os.environ.get('LOG_DAYS', 30))

#    print("CURRENT DB CONFIG: ", db_config, flush=True)

    # Инициализируем Singleton DatabaseManager
    try:
        db_mgr = DatabaseManager(db_config)
    except psycopg2.OperationalError  as err:
        pass # write fatal error log
        sys.exit(1)
    except Exception as err:
        pass
        sys.exit(1)


    # очистка старого логгера
    logger = logging.getLogger("BatchLogger")
    cleanup_logger(logger)

    # Настройка асинхронного логирования через очередь
    log_queue = queue.Queue(-1)
    # Будет записывать в БД сразу по 50 записей
    batch_handler = PostgresBatchHandler(capacity=5, db_manager=db_mgr) 
    listener = logging.handlers.QueueListener(log_queue, batch_handler)
    listener.start()

    logger = logging.getLogger("BatchLogger")
    logger.setLevel(log_level)
    logger.addHandler(logging.handlers.QueueHandler(log_queue))

    # пустой dataframe
    data = pd.DataFrame()
    # Основной блок 
    logger.info(f"{'начало работы скрипта':=^40}")
    # проверим наличие данных в таблице, если их нет то надо заполнить
    last_date = db_mgr.check_table_data()[0]
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
#                df_list.append(cur_df) # -> меняем логику, записываем день сразу в базу
                if cur_df.empty:
                    logger.info(f'Нет данных для записи в БД за {curdate.strftime('%Y-%m-%d')}')
                else:
                    logger.info(f'начало записи данных в БД за {curdate.strftime('%Y-%m-%d')}')
                    db_mgr.write_data(cur_df)
            else:    
                filldata = False
            curdate -= dt_delta    
#        if df_list: # убираем, раз записываем день сразу
#            data = pd.concat(df_list, ignore_index=True)    
    else:
        # заполняем данными за прошлый день
        logger.info('Регулярное заполнение данными')
        delta = (curdate.date() - last_date).days + 1
        for i in range(2, delta):
            logger.info(f"Начало чтения данных по API за {(last_date+(i-1)*dt_delta).strftime('%Y-%m-%d')} ")
            cur_df = last_data(api_url, last_date+i*dt_delta)
#            df_list.append(data)  # Меняем на запись в БД сразу
            if isinstance(cur_df, pd.DataFrame):
                if cur_df.empty:
                    logger.info(f'Нет данных для записи в БД за {curdate.strftime('%Y-%m-%d')}')
                else:
                    logger.info(f'начало записи данных в БД за {curdate.strftime('%Y-%m-%d')}')
                    db_mgr.write_data(cur_df)

#        if df_list:    
#            data = pd.concat(df_list, ignore_index=True)

    # записываем data в БД  - убираем эту часть, пишем каждый день сразу
#    if data.empty:
#        logger.info('Нет данных для записи в БД')
#    else:
#        logger.info('начало записи данных в БД')
#        db_mgr.write_data(data)

    # очистка старых логов
    cln_date = (dt.datetime.now() - cln_days * dt_delta).strftime('%Y-%m-%d')
    db_mgr.log_cleanup(cln_date)
#   по идее надо бы сжать БД

    logger.info(f"{' Завершение работы скрипта ':=^40}")
    logger.info('====&&&&&&&&&&&&&&&&&&&====')
    time.sleep(1)
    batch_handler.flush()
    # Даем время логам записаться перед выходом
    time.sleep(1)
    listener.stop()

    db_instance = DatabaseManager(db_config)
    if db_instance:
        db_instance.close_all()

    del data

    # очиста логгера
    logger = logging.getLogger("BatchLogger")
    cleanup_logger(logger)

if __name__ == "__main__":
    main()