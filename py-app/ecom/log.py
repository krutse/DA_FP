from .db import *
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import logging
import logging.handlers 
import datetime as dt
import sys

# логгер результатов в БД
class PostgresBatchHandler(logging.handlers.BufferingHandler):
    def __init__(self, capacity, db_manager):
        # capacity — сколько логов копим перед отправкой
        super().__init__(capacity)
        # self.db_config = db_config
        self.db_manager = db_manager

    def flush(self):
        """Метод вызывается, когда буфер заполнен или при закрытии"""
        if not self.buffer:
            return

        conn = None
        try:
            # Берем свободное соединение из пула Singleton-а
            conn = self.db_manager.get_connection()
            cur = conn.cursor()
            
            data = []
            for record in self.buffer:
                # Преобразуем timestamp в datetime объект
                timestamp = dt.datetime.fromtimestamp(record.created)
                data.append((timestamp, record.levelname, record.getMessage()))

            # массовая вставка через execute_values
            query = "INSERT INTO ecom.logs (date, level, message) VALUES %s"
            try:
                execute_values(cur, query, data, template="(%s, %s, %s)")
            except Exception as err:
                print(f"Error: {err}")
                sys.exit(1)
            
            conn.commit()
            cur.close()
            
            # Очищаем буфер после успешной записи
            self.buffer = []
        except Exception as err:
            logger.error(f"Ошибка логирования: {err}")

            self.handleError(None)
        finally:
            if conn:
                # Обязательно возвращаем соединение в пул
                self.db_manager.return_connection(conn) 


def cleanup_logger(logger):
    """Полная очистка логгера с закрытием всех ресурсов"""
    for handler in logger.handlers[:]:
        try:
            if hasattr(handler, 'flush'):
                handler.flush()
            if hasattr(handler, 'close'):
                handler.close()
            logger.removeHandler(handler)
        except Exception as e:
            print(f"Error cleaning handler: {e}")           

