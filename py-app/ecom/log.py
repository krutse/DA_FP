import asyncio
import logging
from logging.handlers import QueueHandler, RotatingFileHandler
import queue
import time
from psycopg_pool import AsyncConnectionPool 

class AsyncSplitLogger:   # AsyncPsycopgDBLogger
    def __init__(
        self,
        db_pool,
        file_path="fatalerror.log",
        db_logger_name="db_logger",
        file_logger_name="file_logger",
        batch_size=10,
        flush_interval=2.0
    ):
        """
        :param db_pool: Объект AsyncConnectionPool из библиотеки psycopg_pool
        """
        self.db_pool = db_pool
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        self._db_queue = queue.Queue()
        self._db_logger = logging.getLogger(db_logger_name)
        self._db_logger.setLevel(logging.INFO)
       
        if self._db_logger.hasHandlers():
            self._db_logger.handlers.clear()
            
        self._db_logger.addHandler(QueueHandler(self._db_queue))

        self._file_logger = logging.getLogger(file_logger_name)
        self._file_logger.setLevel(logging.INFO)
        if self._file_logger.hasHandlers():
            self._file_logger.handlers.clear()

        file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        file_handler = RotatingFileHandler(file_path, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
        file_handler.setFormatter(file_formatter)
        self._file_logger.addHandler(file_handler)

        self._worker_task = None
        self._loop = None

    def start(self):
        """Запуск фонового процесса записи логов в БД."""
        self._loop = asyncio.get_running_loop()
        self._worker_task = asyncio.create_task(self._batch_writer_worker())


    # --- МЕТОДЫ ДЛЯ ЗАПИСИ ТОЛЬКО В БАЗУ ДАННЫХ ---
    def info_db(self, msg, *args, **kwargs):
        self._db_logger.info(msg, *args, **kwargs)

    def warning_db(self, msg, *args, **kwargs):
        self._db_logger.warning(msg, *args, **kwargs)

    def error_db(self, msg, *args, **kwargs):
        self._db_logger.error(msg, *args, **kwargs)

    # --- МЕТОДЫ ДЛЯ ЗАПИСИ ТОЛЬКО В ТЕКСТОВЫЙ ФАЙЛ ---
    def info_file(self, msg, *args, **kwargs):
        self._file_logger.info(msg, *args, **kwargs)

    def warning_file(self, msg, *args, **kwargs):
        self._file_logger.warning(msg, *args, **kwargs)

    def error_file(self, msg, *args, **kwargs):
        self._file_logger.error(msg, *args, **kwargs)


    async def _batch_writer_worker(self):
        """Фоновый воркер для сборки пакетов и отправки в psycopg3."""
        insert_query = "INSERT INTO ecom.logs (date, level, message) VALUES (to_timestamp(%s), %s, %s)" 
        
        batch = []
        last_flush_time = time.time()

        try:
            while True:
                try:
                    record = await self._loop.run_in_executor(None, self._db_queue.get_nowait)
                    # record.created возвращает float (timestamp). 
                    # Передаем его как float, в SQL используем функцию to_timestamp()
                    log_entry = (record.created, record.levelname, record.getMessage())
                    batch.append(log_entry)
                except queue.Empty:
                    await asyncio.sleep(0.1)

                time_since_flush = time.time() - last_flush_time
                
                if len(batch) >= self.batch_size or (time_since_flush >= self.flush_interval and batch):
                    await self._write_to_db(insert_query, batch)
                    batch.clear()
                    last_flush_time = time.time()

        except asyncio.CancelledError:
            # Запись логов из буфера при закрытии приложения
            while not self._db_queue.empty():
                try:
                    record = self._db_queue.get_nowait()
                    batch.append((record.created, record.levelname, record.getMessage() ))
                except queue.Empty:
                    break
            
            if batch:
                await self._write_to_db(insert_query, batch)
            print("[Logger] Воркер успешно остановлен, логи сохранены .")

    async def _write_to_db(self, query, batch_data):
        """Асинхронная отправка пачки данных логов"""
        try:
            # Получаем соединение из пула psycopg3
            async with self.db_pool.connection() as conn:
                # Открываем асинхронный курсор
                async with conn.cursor() as cur:
                    await cur.executemany(query, batch_data)
        except Exception as e:
            print(f"[Logger ERROR] Ошибка записи {len(batch_data)} логов в БД: {e}")
#            for item in batch_data:
#                print(f"Потерянный лог: {item}")

    async def stop(self):
        """Остановка логгера."""
        if self._worker_task:
            self._worker_task.cancel()
            await asyncio.gather(self._worker_task, return_exceptions=True)
