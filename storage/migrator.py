from storage.postgres_storage import PostgresStorage
import logging

logger = logging.getLogger(__name__)


# Класс Migrator
# Отвечает за создание и обновление структуры базы данных.
# Основные функции:
#     Создает все необходимые таблицы при первом запуске
#     Создает индексы для ускорения запросов
#     Добавляет триггеры для автоматической обработки событий
#     Заполняет таблицу уровней начальными данными
# Таблицы:
#     users - основная информация о пользователях
#     user_data - дополнительные данные пользователей
#     levels - контент и правила уровней
#     tasks - задания пользователей
#     referrals - реферальные связи
#     donations - информация о донатах
class Migrator:
    def __init__(self, storage: PostgresStorage):
        self.storage = storage

    def migrate(self):
        """Применяет все миграции базы данных"""
        try:
            with self.storage.connection() as conn:
                cursor = conn.cursor()

                base_queries = [
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGINT PRIMARY KEY,
                        registration_complete BOOLEAN NOT NULL DEFAULT FALSE,
                        current_level INTEGER NOT NULL DEFAULT 1,
                        current_state INTEGER NOT NULL DEFAULT 0,
                        registration_date TIMESTAMP DEFAULT NOW()
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS user_data (
                        user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                        name TEXT,
                        birthdate TEXT,
                        location TEXT,
                        language TEXT DEFAULT 'ru',
                        viewed_level INTEGER DEFAULT 1  # <- Новое поле
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS levels (
                        level_number INTEGER PRIMARY KEY,
                        content TEXT NOT NULL,
                        rules TEXT
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS tasks (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                        level INTEGER NOT NULL,
                        task_type TEXT NOT NULL,
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP NOT NULL,
                        completed BOOLEAN NOT NULL DEFAULT FALSE,
                        completion_time TIMESTAMP
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS referrals (
                        id SERIAL PRIMARY KEY,
                        referrer_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                        referee_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                        registration_date TIMESTAMP,
                        UNIQUE (referrer_id, referee_id)
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS donations (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                        level INTEGER NOT NULL,
                        amount DECIMAL(10, 2) NOT NULL,
                        currency TEXT NOT NULL,
                        status TEXT NOT NULL,
                        donation_date TIMESTAMP NOT NULL,
                        payment_id TEXT
                    )
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_tasks_user_level ON tasks(user_id, level)
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_referrals_referee ON referrals(referee_id)
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_donations_user_level ON donations(user_id, level)
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_donations_user_level ON donations(user_id, level) 
                        WHERE status = 'succeeded'
                    """
                ]

                additional_indexes = [
                    """CREATE INDEX IF NOT EXISTS idx_tasks_user_completed 
                       ON tasks(user_id, level, completed)""",
                    """CREATE INDEX IF NOT EXISTS idx_tasks_completion 
                       ON tasks(completed, completion_time)"""
                ]

                new_indexes = [
                    """CREATE INDEX IF NOT EXISTS idx_user_data_viewed_level 
                       ON user_data(viewed_level)""",
                    """CREATE INDEX IF NOT EXISTS idx_user_data_composite 
                       ON user_data(user_id, viewed_level)"""
                ]
                for query in base_queries + additional_indexes + new_indexes:
                    try:
                        logger.info(f"Executing query: {query[:100]}...")
                        cursor.execute(query)
                        conn.commit()
                        logger.info(f"Query executed successfully: {query[:100]}...")
                    except Exception as e:
                        logger.error(f"Ошибка выполнения запроса: {str(e)}")
                        logger.error(f"Failed query: {query}")
                        conn.rollback()
                        continue

                trigger_queries = [
                """
                   CREATE OR REPLACE FUNCTION process_referral_registration()
                RETURNS TRIGGER AS $$
                DECLARE
                    referrer_id BIGINT;
                BEGIN
                    -- Если регистрация завершена и это изменение статуса
                    IF NEW.registration_complete = TRUE AND 
                       (OLD.registration_complete IS DISTINCT FROM NEW.registration_complete) THEN
                        
                        -- Находим referrer_id для этого пользователя (даже если запись была создана после первого входа)
                        SELECT r.referrer_id INTO referrer_id 
                        FROM referrals r 
                        WHERE r.referee_id = NEW.id
                        ORDER BY r.id DESC  -- Берем самую свежую запись
                        LIMIT 1;
                        
                        IF referrer_id IS NOT NULL THEN
                            -- Обновляем дату регистрации, если она NULL
                            UPDATE referrals 
                            SET registration_date = NEW.registration_date
                            WHERE referee_id = NEW.id AND registration_date IS NULL;
                            
                            -- Создаем задание для реферера
                            INSERT INTO tasks (
                                user_id, level, task_type, 
                                start_time, end_time, completed
                            ) VALUES (
                                referrer_id, 
                                (SELECT current_level FROM users WHERE id = referrer_id),
                                'referral',
                                NOW(),
                                NOW(),
                                TRUE
                            );
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
                ]

                for query in trigger_queries:
                    try:
                        cursor.execute(query)
                        conn.commit()
                        logger.info("Триггер успешно создан/обновлен")
                    except Exception as e:
                        logger.error(f"Ошибка создания триггера: {str(e)}")
                        conn.rollback()
                        continue

                try:
                    cursor.execute("SELECT 1 FROM levels LIMIT 1")
                    if not cursor.fetchone():
                        for level in range(1, 22):
                            cursor.execute(
                                "INSERT INTO levels (level_number, content, rules) VALUES (%s, %s, %s)",
                                (level, f"Контент для уровня {level}", f"Правила уровня {level}")
                            )
                        conn.commit()
                except Exception as e:
                    logger.error(f"Ошибка заполнения уровней: {str(e)}")
                    conn.rollback()

                logger.info("Миграции PostgreSQL выполнены успешно")

        except Exception as e:
            logger.error(f"Критическая ошибка при выполнении миграций: {str(e)}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    migrator = Migrator(PostgresStorage("probuzhdenie", "postgres", "5g", "localhost", "5433"))
    migrator.migrate()
