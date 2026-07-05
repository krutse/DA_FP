import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import threading, logging, os
from datetime import datetime

# пул соединений к БД
class DatabaseManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_config=None):
        
        if cls._instance is not None:
            return cls._instance        
        
        with cls._lock:
            if cls._instance is None:
                try:
                    instance = super(DatabaseManager, cls).__new__(cls)
                    # Инициализируем пул соединений (от 1 до 10 потоков)
                    instance.pool = psycopg2.pool.ThreadedConnectionPool(1, 10, **db_config)

                    cls._instance = instance
                    # logging.info("Пул соединений с БД успешно создан")
                    return cls._instance
                
                except Exception as err:    
                    error_msg = f'КРИТИЧЕСКАЯ ОШИБКА: Не удалось создать пул соединений с БД: {err}'
                    print("Critical ERROR: ", error_msg, flush=True)
                    # cls._log_fatal_error(error_msg)
                    log_dir = './log'
                    if not os.path.exists(log_dir):
                        try:
                            os.makedirs(log_dir)
                        except:
                            pass                    
                    log_file = os.path.join(log_dir, 'fatalerror.log')
                    try:
                        with open(log_file, 'w', encoding='utf-8') as f:
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            f.write(f"[{timestamp}] {error_msg}\n")
                    except:
                        pass
                    cls._instance = None
                    raise

        return cls._instance

    def get_connection(self):
        return self.pool.getconn()

    def return_connection(self, conn):
        self.pool.putconn(conn)

    def close_all(self):
        with self._lock:
            if self.pool:
                self.pool.closeall()
            DatabaseManager._instance = None  # Сбрасываем синглтон    

    @staticmethod
    def _log_fatal_error(message):
        """Внутренний метод для записи фатальных ошибок"""
        log_dir = './log'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_file = os.path.join(log_dir, 'fatalerror.log')
        try:
            with open(log_file, 'w', encoding='utf-8') as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"[{timestamp}] FATAL: {message}\n")
        except Exception as e:
            pass

    # проверка таблицы данных на пустоту
    def check_table_data(self):
        sql = """select coalesce(max(purchase_datetime), '1970-01-01') as date from ecom.sales_details"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as c:
                c.execute(sql)
                res = c.fetchone()
        except Exception as err:
            logger.error(f"Ошибка соединения check_table_data: {err}")     
            res = None   
        finally:    
            if conn:
                self.return_connection(conn)  # возвращаем соединение в пул
        return res

    # очиста старых log записей
    def log_cleanup(self, cln_date):
        sql = """delete from ecom.logs where date::date <= %s"""
        sql_vacuum = """vacuum full ecom.logs;"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as c:
                c.execute(sql, (cln_date, ))
            conn.commit()
        except Exception as err:
            logger.error(f"Ошибка соединения log_cleanup: {err}")  
        finally:    
            if conn:
                self.return_connection(conn)  # возвращаем соединение в пул
        # освобождение места после удаления
        try:
            conn = self.get_connection()
            conn.autocommit = True
            with conn.cursor() as c:
                c.execute(sql_vaquum)
        except Exception as err:
            logger.error(f'Ошибка очистки данных (vacuum full): {err}')
        finally:
            if conn:
                self.return_connection(conn)

    # запись данных в БД
    def write_data(self, df):  #, db_mgr, logger
        conn = None
        try:
            conn = self.get_connection()
            data_tuples = [tuple(x) for x in df.to_numpy()]
            cols = ','.join(list(df.columns))
            sql = f"insert into ecom.sales_details ({cols}) values %s"
            with conn.cursor() as c:
                execute_values(c, sql, data_tuples)
            conn.commit()
            #logging.info(f'')
        except Exception as err:
            logger.error(f"Ошибка соединения write_data: {err}") 
        finally:
            self.return_connection(conn)
            logger.info("Данные записаны в БД")
        
        

