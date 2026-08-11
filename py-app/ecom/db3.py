import asyncio
import os
from datetime import datetime
import psycopg
from psycopg.rows import scalar_row
from psycopg_pool import AsyncConnectionPool  
import logging

# logger = logging.getLogger('BatchLogger')  
# file_log = logging.getLogger("file_logger")

# Если логгер не настроен, создаем базовый
#if not logger.handlers:
#    logging.basicConfig(level=logging.ERROR)
#    logger = logging.getLogger('BatchLogger')

# db_logger_name=""
logger = logging.getLogger(__name__)

# Пул соединений к БД (Асинхронный синглтон)
class DatabaseManager:
    _instance = None
    _lock = asyncio.Lock()  

    def __init__(self, db_config=None):
        self._pool = None
        self.db_config = db_config

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._pool = None
            cls._instance.db_config = kwargs.get('db_config')
        return cls._instance

    async def initialize_pool(self, db_config):
        """Асинхронная инициализация пула"""
        async with self._lock:
            if self._pool is not None:
                return

            try:
                self._pool = AsyncConnectionPool(
                    kwargs=db_config,
                    min_size=1,  # границы пула
                    max_size=10, # границы пула
                    open=False 
                )
                # Явно открываем пул (происходит проверка подключения)
                await self._pool.open()
                # logging.info("Асинхронный пул соединений с БД успешно создан")
                await self._pool.wait()  # Ожидаем готовности соединений
                # logging.info("Пул psycopg3 успешно открыт.")
            
            except Exception as err:    
                error_msg = f'КРИТИЧЕСКАЯ ОШИБКА: Не удалось создать пул соединений с БД: {err}'
                file_log.error(error_msg)
                print("Critical ERROR: ", error_msg, flush=True)
#                _log_fatal_error(error_message)
                
#                log_dir = './log'
#                if not os.path.exists(log_dir):
#                    try:
#                        os.makedirs(log_dir)
#                    except:
#                        pass                    
#                log_file = os.path.join(log_dir, 'fatalerror.log')
#                try:
#                    with open(log_file, 'a', encoding='utf-8') as f:
#                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                        f.write(f"[{timestamp}] {error_msg}\n")
#                except:
#                    pass
                self._pool = None
                raise

    @property
    def pool(self) -> AsyncConnectionPool:
        """Отдаем пул наружу для нашего логгера."""
        if self._pool is None:
            raise RuntimeError("База данных не подключена.")
        return self._pool


    async def close_all(self):
        async with self._lock:
            if self._pool:
                await self._pool.close()
            DatabaseManager._instance = None  # Сбрасываем синглтон

    @staticmethod
    def _log_fatal_error(message):
        """Внутренний метод для записи фатальных ошибок"""
        log_dir = './log'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_file = os.path.join(log_dir, 'fatalerror.log')
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"[{timestamp}] FATAL: {message}\n")
        except Exception as e:
            pass

    # Проверка таблицы данных на пустоту
    async def check_table_data(self):
        sql = """select coalesce(max(purchase_datetime), '1970-01-01') as date from ecom.sales_details"""
        try:
            # async with self._pool.connection() берет соединение из пула 
            # и автоматически возвращает его обратно при выходе из блока.
            async with self._pool.connection() as conn:
                async with conn.cursor() as c:
                    await c.execute(sql)
                    res = await c.fetchone()
        except Exception as err:
            logger.error(f"Ошибка соединения check_table_data: {err}")     
            res = None   
        return res

    # Список пропущенных дат
    async def missing_dates(self, min_date):
        sql = f"""
            with full_dt_list as (
	        select dt::date as calendar_date
	        from generate_series('{min_date}'::date, (now() - interval '1 day')::date, '1 day'::interval ) dt
            )
            select fl.calendar_date 
            from full_dt_list fl 
            left join ecom.v_calendar vc on vc.calendar_date = fl.calendar_date 
            where vc.calendar_date is null
        """
        try:
            async with self._pool.connection() as conn:  #
                async with conn.cursor(row_factory=scalar_row) as c:    # row_factory для плоского списка 
                    await c.execute(sql)
                    res = await c.fetchall() # [r[0] for r in await c.fetchall()]   # переделываем список кортежей в простой список
            # pass
        except Exception as err:
            logger.error(f'{err}')
            res = None
        return res

    # Очистка старых log записей
    async def log_cleanup(self, cln_date):
        sql = """delete from ecom.logs where date::date <= %s"""
        sql_vacuum = """vacuum full ecom.logs;"""
        
        # 1. Удаление логов (в транзакции)
        try:
            async with self._pool.connection() as conn:
                async with conn.cursor() as c:
                    await c.execute(sql, (cln_date, ))
        except Exception as err:
            logger.error(f"Ошибка соединения log_cleanup: {err}")  

        # 2. Освобождение места (Вне транзакции — VACUUM требует autocommit)
        try:
            async with self._pool.connection() as conn:
                await conn.set_autocommit(True)
                async with conn.cursor() as c:
                    await c.execute(sql_vacuum)
        except Exception as err:
            logger.error(f'Ошибка очистки данных (vacuum full): {err}')

    # Запись данных в БД
    async def write_data(self, df):
        try:
            async with self._pool.connection() as conn:
                data_tuples = [tuple(x) for x in df.to_numpy()]
                cols = ','.join(list(df.columns))
                placeholders = ', '.join(['%s'] * len(df.columns))
                sql = f"insert into ecom.sales_details ({cols}) values ({placeholders})"
                
                async with conn.cursor() as c:
                    await c.executemany(sql, data_tuples)
                    
            logger.info("Данные записаны в БД")
        except Exception as err:
            logger.error(f"Ошибка соединения write_data: {err}") 


#    def get_connection_sync(self):
#        """Синхронное получение соединения из асинхронного пула"""
#        if self._pool is None:
#            raise Exception("Database pool is not initialized")
#    
#        # Проверяем, есть ли запущенный event loop
#        try:
#            loop = asyncio.get_running_loop()
#            # Если loop запущен, используем run_coroutine_threadsafe
#            future = asyncio.run_coroutine_threadsafe(
#                self._get_connection_async(),
#                loop
#            )
#            return future.result(timeout=5)  # Ждем 5 секунд
#        except RuntimeError:
#            # Нет запущенного loop - создаем новый
#            loop = asyncio.new_event_loop()
#            asyncio.set_event_loop(loop)
#            try:
#                conn = loop.run_until_complete(self._get_connection_async())
#                return conn
#            finally:
#                loop.close()
#
#    async def _get_connection_async(self):
#        """Асинхронное получение соединения"""
#        return await self._pool.get_connection()

#    def return_connection_sync(self, conn):
#        """Синхронное возвращение соединения в пул"""
#        try:
#            loop = asyncio.get_running_loop()
#            future = asyncio.run_coroutine_threadsafe(
#                self._return_connection_async(conn),
#                loop
#            )
#            future.result(timeout=5)
#        except RuntimeError:
#            loop = asyncio.new_event_loop()
#            asyncio.set_event_loop(loop)
#            try:
#                loop.run_until_complete(self._return_connection_async(conn))
#            finally:
#                loop.close()
#
#    async def _return_connection_async(self, conn):
#        """Асинхронное возвращение соединения"""
#        await self._pool.put_connection(conn)
