from storage.postgres_storage import PostgresStorage
import logging

logger = logging.getLogger(__name__)


class Migrator:
    def __init__(self, storage: PostgresStorage):
        self.storage = storage

    def migrate(self):
        """Применяет все миграции базы данных"""
        try:
            with self.storage.connection() as conn:
                cursor = conn.cursor()

                # 1. Очистка дубликатов
                self._clean_duplicates(cursor, conn)

                # 2. Создание таблиц
                base_queries = [
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGINT PRIMARY KEY,
                        registration_complete BOOLEAN NOT NULL DEFAULT FALSE,
                        current_level INTEGER NOT NULL DEFAULT 1 
                            CHECK (current_level >= 1 AND current_level <= 21),
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
                        viewed_level INTEGER DEFAULT 1 
                            CHECK (viewed_level >= 1 AND viewed_level <= 21)
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS levels (
                        level_number INTEGER PRIMARY KEY 
                            CHECK (level_number >= 1 AND level_number <= 21),
                        content TEXT NOT NULL,
                        rules TEXT
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS tasks (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        level INTEGER NOT NULL 
                            CHECK (level >= 1 AND level <= 21),
                        task_type TEXT NOT NULL 
                            CHECK (task_type IN ('time', 'referral', 'donation', 'auto')),
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP NOT NULL,
                        completed BOOLEAN NOT NULL DEFAULT FALSE,
                        completion_time TIMESTAMP,
                        CONSTRAINT tasks_unique UNIQUE (user_id, level, task_type),
                        CONSTRAINT valid_task_times CHECK (end_time >= start_time)
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS referrals (
                        id SERIAL PRIMARY KEY,
                        referrer_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        referee_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        level INTEGER NOT NULL 
                            CHECK (level >= 1 AND level <= 21),
                        referral_date TIMESTAMP DEFAULT NOW(),
                        registration_date TIMESTAMP,
                        CONSTRAINT referrals_unique UNIQUE (referrer_id, referee_id, level),
                        CONSTRAINT no_self_referral CHECK (referrer_id != referee_id)
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS donations (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        level INTEGER NOT NULL 
                            CHECK (level >= 1 AND level <= 21),
                        amount DECIMAL(10, 2) NOT NULL 
                            CHECK (amount > 0),
                        currency TEXT NOT NULL,
                        status TEXT NOT NULL,
                        donation_date TIMESTAMP NOT NULL,
                        payment_id TEXT,
                        processed BOOLEAN DEFAULT FALSE
                    )
                    """
                ]

                for query in base_queries:
                    cursor.execute(query)
                    conn.commit()

                # 3. Создание индексов
                index_queries = [
                    "CREATE INDEX IF NOT EXISTS idx_tasks_user_level ON tasks(user_id, level)",
                    "CREATE INDEX IF NOT EXISTS idx_referrals_referrer_level ON referrals(referrer_id, level)",
                    "CREATE INDEX IF NOT EXISTS idx_referrals_referee_level ON referrals(referee_id, level)",
                    "CREATE INDEX IF NOT EXISTS idx_referrals_level ON referrals(level)",
                    "CREATE INDEX IF NOT EXISTS idx_donations_user_level ON donations(user_id, level)",
                    """CREATE INDEX IF NOT EXISTS idx_donations_user_level_succeeded 
                       ON donations(user_id, level) WHERE status = 'succeeded'""",
                    "CREATE INDEX IF NOT EXISTS idx_tasks_user_completed ON tasks(user_id, level, completed)",
                    "CREATE INDEX IF NOT EXISTS idx_tasks_completion ON tasks(completed, completion_time)",
                    "CREATE INDEX IF NOT EXISTS idx_user_data_viewed_level ON user_data(viewed_level)",
                    "CREATE INDEX IF NOT EXISTS idx_user_data_composite ON user_data(user_id, viewed_level)",
                    "CREATE INDEX IF NOT EXISTS idx_referrals_registration ON referrals(registration_date)",
                    "CREATE INDEX IF NOT EXISTS idx_users_registration ON users(registration_complete)",
                    "CREATE INDEX IF NOT EXISTS idx_donations_payment_id ON donations(payment_id) WHERE payment_id "
                    "IS NOT NULL",
                ]

                for query in index_queries:
                    cursor.execute(query)
                    conn.commit()

                cursor.execute("""
                    DROP TRIGGER IF EXISTS after_user_registration ON users
                """)
                conn.commit()

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION process_referral_registration()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        updated_referrals INT;
                        inserted_tasks INT;
                    BEGIN
                        -- Обновляем реферальные записи
                        UPDATE referrals 
                        SET registration_date = NOW()
                        WHERE referee_id = NEW.id 
                        AND registration_date IS NULL;

                        GET DIAGNOSTICS updated_referrals = ROW_COUNT;

                        -- Если есть обновленные рефералы, создаем/обновляем задачи
                        IF updated_referrals > 0 THEN
                            -- Вставка или обновление существующих задач
                            INSERT INTO tasks (
                                user_id, level, task_type, 
                                start_time, end_time, completed,
                                completion_time
                            )
                            SELECT 
                                r.referrer_id, 
                                r.level, 
                                'referral', 
                                NOW(), 
                                NOW(), 
                                TRUE,
                                NOW()
                            FROM referrals r
                            WHERE r.referee_id = NEW.id
                            AND r.registration_date IS NOT NULL
                            ON CONFLICT (user_id, level, task_type) 
                            DO UPDATE SET
                                completed = EXCLUDED.completed,
                                completion_time = EXCLUDED.completion_time,
                                end_time = EXCLUDED.end_time;

                            GET DIAGNOSTICS inserted_tasks = ROW_COUNT;

                            -- Логирование в системную таблицу
                            INSERT INTO audit_log (event_type, user_id, message)
                            VALUES ('referral_processed', NEW.id, 
                                'Processed ' || inserted_tasks || ' referral tasks');
                        END IF;

                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)
                conn.commit()

                cursor.execute("""
                    CREATE TRIGGER after_user_registration
                    AFTER UPDATE OF registration_complete ON users
                    FOR EACH ROW
                    WHEN (NEW.registration_complete IS TRUE AND 
                         (OLD.registration_complete IS FALSE OR OLD.registration_complete IS NULL))
                    EXECUTE FUNCTION process_referral_registration();
                """)
                conn.commit()

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS audit_log (
                        id SERIAL PRIMARY KEY,
                        event_time TIMESTAMP DEFAULT NOW(),
                        event_type TEXT NOT NULL,
                        user_id BIGINT,
                        message TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_audit_log_event_type ON audit_log(event_type);
                    CREATE INDEX IF NOT EXISTS idx_audit_log_user_id ON audit_log(user_id);
                """)
                conn.commit()

                cursor.execute("""
                    UPDATE tasks t
                    SET 
                        completed = TRUE,
                        completion_time = NOW(),
                        end_time = NOW()
                    FROM referrals r
                    WHERE 
                        t.user_id = r.referrer_id
                        AND t.level = r.level
                        AND t.task_type = 'referral'
                        AND r.registration_date IS NOT NULL
                        AND t.completed = FALSE;
                """)
                conn.commit()

                logger.info("Все миграции успешно выполнены")

        except Exception as e:
            logger.error(f"Ошибка при выполнении миграций: {str(e)}", exc_info=True)
            raise

    def _clean_duplicates(self, cursor, conn):
        """Очистка дубликатов в основных таблицах"""
        try:
            logger.info("Очистка возможных дубликатов в таблице tasks...")
            cursor.execute("""
                DELETE FROM tasks 
                WHERE ctid NOT IN (
                    SELECT min(ctid) 
                    FROM tasks 
                    GROUP BY user_id, level, task_type
                )
            """)
            conn.commit()

            logger.info("Очистка возможных дубликатов в таблице referrals...")
            cursor.execute("""
                DELETE FROM referrals 
                WHERE ctid NOT IN (
                    SELECT min(ctid) 
                    FROM referrals 
                    GROUP BY referrer_id, referee_id, level
                )
            """)
            conn.commit()

            logger.info("Очистка дубликатов завершена")
        except Exception as e:
            logger.warning(f"Не удалось очистить дубликаты: {str(e)}")
            conn.rollback()

    def _verify_data_integrity(self, cursor, conn):
        """Базовая проверка целостности данных"""
        try:
            logger.info("Проверка целостности данных...")

            # Проверка рефералов с несуществующими пользователями
            cursor.execute("""
                SELECT COUNT(*) FROM referrals r
                LEFT JOIN users u1 ON r.referrer_id = u1.id
                LEFT JOIN users u2 ON r.referee_id = u2.id
                WHERE u1.id IS NULL OR u2.id IS NULL
            """)
            invalid_referrals = cursor.fetchone()[0]
            if invalid_referrals > 0:
                logger.warning(f"Найдены невалидные реферальные связи: {invalid_referrals}")

            logger.info("Проверка целостности данных завершена")
        except Exception as e:
            logger.error(f"Ошибка при проверке целостности данных: {str(e)}")
            conn.rollback()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    migrator = Migrator(PostgresStorage("probuzhdenie", "postgres", "5g", "localhost", "5433"))
    migrator.migrate()