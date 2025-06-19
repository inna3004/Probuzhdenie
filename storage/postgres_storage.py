import psycopg2
from contextlib import contextmanager
from typing import Optional, Iterator
import logging

logger = logging.getLogger(__name__)


class PostgresStorage:
    def __init__(self, dbname: str, user: str, password: str,
                 host: str = 'localhost', port: str = '5433'):
        """
        Инициализация подключения к PostgreSQL
        :param port: порт (по умолчанию 5433)
        """
        self.connection_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port,
            'client_encoding': 'UTF8',
            'connect_timeout': 5
        }

    @contextmanager
    def connection(self) -> Iterator[psycopg2.extensions.connection]:
        """
        Контекстный менеджер для работы с подключением к БД
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            yield conn
        except psycopg2.OperationalError as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise RuntimeError("Не удалось подключиться к базе данных") from e
        except psycopg2.Error as e:
            logger.error(f"Ошибка PostgreSQL: {e}")
            raise
        finally:
            if conn is not None and not conn.closed:
                conn.close()

    def test_connection(self) -> bool:
        """Проверяет доступность базы данных"""
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return cursor.fetchone()[0] == 1
        except Exception:
            return False



    def close(self) -> None:
        pass
