import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
from service.config import MAX_LEVEL
from service.states import BotStates
from storage.postgres_storage import PostgresStorage

logger = logging.getLogger(__name__)


# Базовый класс BaseRepository
# Базовый класс для всех репозиториев, содержащий общую логику:
# Принимает экземпляр PostgresStorage для работы с базой данных
# Предоставляет доступ к соединению через self.storage.connection()
class BaseRepository:
    def __init__(self, storage: PostgresStorage):
        self.storage = storage


# Класс UserRepository
# Работает с таблицей пользователей (users):
# Основные методы:
# get_user(user_id) - получает данные пользователя по ID
# create_user(user_id) - создает нового пользователя с начальными значениями
# set_user_state(user_id, state) - обновляет текущее состояние пользователя
# get_user_state(user_id) - получает текущее состояние пользователя
# complete_registration(user_id) - отмечает регистрацию пользователя как завершенную
# update_user_level(user_id, level) - обновляет текущий уровень пользователя
class UserRepository(BaseRepository):
    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получить пользователя по ID"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT * FROM users WHERE id = %s",
                    (user_id,)
                )
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                logger.error(f"Error getting user {user_id}: {e}")
                return None

    def create_user(self, user_id: int) -> bool:
        """Создать нового пользователя"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """INSERT INTO users (id, registration_complete, current_level, current_state) 
                    VALUES (%s, FALSE, 1, 0)
                    ON CONFLICT (id) DO NOTHING""",
                    (user_id,)
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Error creating user {user_id}: {e}")
                conn.rollback()
                return False

    def set_user_state(self, user_id: int, state: int) -> bool:
        """Установить состояние пользователя"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """UPDATE users SET current_state = %s 
                    WHERE id = %s""",
                    (state, user_id)
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Error setting state for user {user_id}: {e}")
                conn.rollback()
                return False

    def get_user_state(self, user_id: int) -> Optional[int]:
        """Получить текущее состояние пользователя"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT current_state FROM users WHERE id = %s",
                    (user_id,)
                )
                result = cursor.fetchone()
                logger.info(f"DB Query - User {user_id} state: {result[0] if result else None}")
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Error getting state for user {user_id}: {e}")
                return None

    def complete_registration(self, user_id: int) -> bool:
        """Завершить регистрацию пользователя"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """UPDATE users SET registration_complete = TRUE, 
                    current_level = 1, current_state = %s 
                    WHERE id = %s""",
                    (BotStates.MAIN_MENU, user_id)
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Error completing registration for user {user_id}: {e}")
                conn.rollback()
                return False

    def update_user_level(self, user_id: int, level: int, force: bool = False) -> bool:
        """Обновить текущий уровень пользователя с проверкой на MAX_LEVEL"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                if not force:
                    # Получаем текущий уровень для проверки
                    cursor.execute(
                        "SELECT current_level FROM users WHERE id = %s",
                        (user_id,)
                    )
                    current_level = cursor.fetchone()[0]

                    # Проверяем, что новый уровень не превышает MAX_LEVEL
                    if level > MAX_LEVEL:
                        logger.warning(f"Attempt to set level {level} which exceeds MAX_LEVEL {MAX_LEVEL}")
                        return False

                    if level <= current_level:
                        logger.info(f"Level {level} not greater than current {current_level}, skipping update")
                        return False

                cursor.execute(
                    "UPDATE users SET current_level = %s WHERE id = %s",
                    (min(level, MAX_LEVEL), user_id)  # Гарантируем, что уровень не превысит MAX_LEVEL
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Error updating level for user {user_id}: {e}")
                conn.rollback()
                return False
# Класс UserDataRepository
# Работает с дополнительными данными пользователей (user_data):
# Основные методы:
# save_user_data(user_id, **kwargs) - сохраняет или обновляет данные пользователя
# get_user_data(user_id) - получает все дополнительные данные пользователя

class UserDataRepository(BaseRepository):
    def save_user_data(self, user_id: int, **kwargs) -> bool:
        """Сохранить данные пользователя"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1 FROM user_data WHERE user_id = %s", (user_id,))
                exists = cursor.fetchone()

                if exists:
                    set_clause = ", ".join([f"{k} = %s" for k in kwargs.keys()])
                    values = list(kwargs.values()) + [user_id]
                    cursor.execute(
                        f"UPDATE user_data SET {set_clause} WHERE user_id = %s",
                        values
                    )
                else:
                    columns = ["user_id"] + list(kwargs.keys())
                    placeholders = ["%s"] * len(columns)
                    values = [user_id] + list(kwargs.values())
                    cursor.execute(
                        f"INSERT INTO user_data ({', '.join(columns)}) VALUES ({', '.join(placeholders)})",
                        values
                    )

                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error saving user data for {user_id}: {e}")
                conn.rollback()
                return False

    def get_user_data(self, user_id: int) -> Dict[str, Any]:
        """Получить все данные пользователя"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT * FROM user_data WHERE user_id = %s",
                    (user_id,)
                )
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, result))
                return {}
            except Exception as e:
                logger.error(f"Error getting user data for {user_id}: {e}")
                return {}

    def set_viewed_level(self, user_id: int, level: int) -> bool:
        """Устанавливает уровень, который пользователь сейчас просматривает"""
        return self.save_user_data(user_id=user_id, viewed_level=level)

    def get_viewed_level(self, user_id: int) -> int:
        """Получает уровень, который пользователь сейчас просматривает"""
        data = self.get_user_data(user_id)
        return data.get('viewed_level', 1)


# Класс LevelRepository
# Работает с уровнями (levels):
# Основные методы:
# get_level_content(level_number) - получает контент для указанного уровня
# get_level_rules(level_number) - получает правила для указанного уровня
class LevelRepository(BaseRepository):
    def get_level_content(self, level_number: int) -> Optional[str]:
        """Получить контент для указанного уровня"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT content FROM levels WHERE level_number = %s",
                    (level_number,)
                )
                result = cursor.fetchone()
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Error getting content for level {level_number}: {e}")
                return None

    def get_level_rules(self, level_number: int) -> Optional[str]:
        """Получить правила для указанного уровня"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT rules FROM levels WHERE level_number = %s",
                    (level_number,)
                )
                result = cursor.fetchone()
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Error getting rules for level {level_number}: {e}")
                return None


# Класс TaskRepository
# Работает с заданиями пользователей (tasks):
# Основные методы:
# create_task(user_id, level, task_type, start_time, end_time) - создает новое задание
# get_active_time_task(user_id, level) - получает активное задание на время
# complete_task(user_id, level, task_type) - отмечает задание как выполненное
# is_task_completed(user_id, level, task_type) - проверяет выполнение задания
#
class TaskRepository(BaseRepository):
    def create_task(self, user_id: int, level: int, task_type: str,
                    start_time: datetime, end_time: datetime, completed: bool = False) -> bool:
        """Создать задание с возможностью сразу отметить как выполненное"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """INSERT INTO tasks 
                    (user_id, level, task_type, start_time, end_time, completed) 
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    (user_id, level, task_type, start_time, end_time, completed)
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Error creating task: {e}")
                conn.rollback()
                return False

    def has_level_records(self, user_id: int, level: int) -> bool:
        """Проверяет, есть ли записи для указанного уровня"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT 1 FROM tasks 
                    WHERE user_id = %s AND level = %s LIMIT 1""",
                    (user_id, level)
                )
                return cursor.fetchone() is not None
            except Exception as e:
                logger.error(f"Error checking level records in tasks: {e}")
                return False

    def get_completed_levels(self, user_id: int) -> List[int]:
        """Получить список уровней, для которых выполнены задания"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT level FROM tasks 
                WHERE user_id = %s AND completed = TRUE
                ORDER BY level
            """, (user_id,))
            return [row[0] for row in cursor.fetchall()]

    def get_active_time_task(self, user_id: int, level: int) -> Optional[Dict[str, Any]]:
        """Получить активное задание на время для пользователя и уровня"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT * FROM tasks 
                    WHERE user_id = %s AND level = %s 
                    AND task_type = 'time' AND completed = FALSE
                    ORDER BY start_time DESC LIMIT 1""",
                    (user_id, level)
                )
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                logger.error(f"Error getting active time task for user {user_id}: {e}")
                return None

    def complete_donation_task(self, user_id: int, donation_level: int) -> bool:
        """Отметить донатное задание как выполненное для текущего уровня"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                # Проверяем существование доната именно для этого уровня
                cursor.execute("""
                    SELECT 1 FROM donations 
                    WHERE user_id = %s AND level = %s AND status = 'succeeded'
                    LIMIT 1
                """, (user_id, donation_level))

                if not cursor.fetchone():
                    logger.error(f"No succeeded donation for level {donation_level}")
                    return False

                # Создаем запись о выполнении задания для ТЕКУЩЕГО уровня
                cursor.execute("""
                    INSERT INTO tasks 
                    (user_id, level, task_type, start_time, end_time, completed)
                    VALUES (%s, %s, 'donation', NOW(), NOW(), TRUE)
                    ON CONFLICT (user_id, level, task_type) DO NOTHING
                """, (user_id, donation_level))

                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                conn.rollback()
                logger.error(f"Error completing donation task: {e}")
                return False

    def complete_task(self, user_id: int, level: int, task_type: str) -> bool:
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """UPDATE tasks SET completed = TRUE, 
                    completion_time = NOW() 
                    WHERE user_id = %s AND level = %s 
                    AND task_type = %s AND completed = FALSE""",
                    (user_id, level, task_type)
                )
                conn.commit()
                logger.info(f"Task completed: user={user_id}, level={level}, type={task_type}")
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Error completing task: {e}")
                conn.rollback()
                return False

    def is_task_completed(self, user_id: int, level: int, task_type: str = None) -> bool:
        """Проверить, выполнено ли задание для конкретного уровня и типа"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                logger.info(f"[Task Check] Checking tasks for user {user_id}, level {level}, type {task_type}")

                query = """SELECT id, task_type, completed, completion_time FROM tasks 
                        WHERE user_id = %s AND level = %s 
                        AND completed = TRUE"""
                params = [user_id, level]

                if task_type:
                    query += " AND task_type = %s"
                    params.append(task_type)

                cursor.execute(query, params)
                result = cursor.fetchall()

                if result:
                    logger.info(f"[Task Check] Found completed tasks: {result}")
                    return True
                else:
                    logger.info("[Task Check] No completed tasks found")
                    return False

            except Exception as e:
                logger.error(f"[Task Check] Error checking task completion: {str(e)}")
                return False


def is_task_completed_for_level(self, user_id: int, level: int) -> bool:
    """Проверить, выполнено ли задание для конкретного уровня"""
    with self.storage.connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """SELECT 1 FROM tasks 
                WHERE user_id = %s AND level = %s 
                AND completed = TRUE""",
                (user_id, level)
            )
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking task completion for level: {e}")
            return False


# Класс ReferralRepository
# Работает с реферальными связями (referrals):
# Основные методы:
# create_referral(referrer_id, referee_id) - создает реферальную связь
# get_referral_status(user_id, level) - получает статус рефералов пользователя
class ReferralRepository(BaseRepository):
    def create_referral(self, referrer_id: int, referee_id: int) -> bool:
        """Создать реферальную связь с проверкой на дубликаты"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                # Проверяем существование referrer
                cursor.execute("SELECT 1 FROM users WHERE id = %s", (referrer_id,))
                if not cursor.fetchone():
                    return False

                # Проверяем существование referee (даже если регистрация не завершена)
                cursor.execute("SELECT 1 FROM users WHERE id = %s", (referee_id,))
                if not cursor.fetchone():
                    # Создаем пользователя если его нет
                    cursor.execute(
                        "INSERT INTO users (id) VALUES (%s) ON CONFLICT DO NOTHING",
                        (referee_id,)
                    )
                    conn.commit()

                # Проверяем существование записи
                cursor.execute(
                    "SELECT 1 FROM referrals WHERE referrer_id = %s AND referee_id = %s",
                    (referrer_id, referee_id)
                )
                if cursor.fetchone():
                    return False

                cursor.execute(
                    """INSERT INTO referrals 
                    (referrer_id, referee_id, registration_date) 
                    VALUES (%s, %s, NULL)""",
                    (referrer_id, referee_id)
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error creating referral {referrer_id}->{referee_id}: {e}")
                conn.rollback()
                return False

    def get_referral_status(self, user_id: int, level: int) -> dict:
        """Получить статус рефералов для пользователя и уровня"""
        try:
            with self.storage.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) FROM referrals
                        WHERE referrer_id = %s
                    """, (user_id,))
                    total = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM referrals r
                        JOIN users u ON r.referee_id = u.id
                        WHERE r.referrer_id = %s
                        AND u.registration_complete = TRUE
                        AND r.registration_date IS NOT NULL
                    """, (user_id,))
                    completed = cursor.fetchone()[0]

                    return {
                        'total_referrals': total,
                        'completed_referrals': completed,
                        'pending_referrals': total - completed,
                        'error': None
                    }
        except Exception as e:
            logger.error(f"Error in get_referral_status: {str(e)}")
            return {
                'total_referrals': 0,
                'completed_referrals': 0,
                'pending_referrals': 0,
                'error': str(e)
            }


# Класс DonationRepository
# Работает с донатами (donations):
# Константы статусов:
#     STATUS_PENDING - ожидание
#     STATUS_WAITING_FOR_CAPTURE - ожидание подтверждения
#     STATUS_SUCCEEDED - успешно завершен
#     STATUS_CANCELED - отменен
#
# Основные методы:
#     create_donation(user_id, level, amount, currency, status, payment_id) - создает запись о донате
#     get_last_donation(user_id, level) - получает последний донат пользователя
#     update_donation_status(donation_id, status, payment_id) - обновляет статус доната
#     get_donation_by_payment_id- ищет донаты по payment_id для корректного обновления
#     get_pending_payments-получение необработанных платежей
#     get_charity_donations-получение всеx благотворительные пожертвования пользователя"""
class DonationRepository(BaseRepository):
    STATUS_PENDING = 'pending'
    STATUS_WAITING_FOR_CAPTURE = 'waiting_for_capture'
    STATUS_SUCCEEDED = 'succeeded'
    STATUS_CANCELED = 'canceled'

    def create_donation(self, user_id: int, for_level: int, amount: float,
                        currency: str, status: str, payment_id: str = None) -> bool:
        """Создать запись о донате для перехода на указанный уровень"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """INSERT INTO donations 
                    (user_id, level, amount, currency, status, donation_date, payment_id) 
                    VALUES (%s, %s, %s, %s, %s, NOW(), %s)""",
                    (user_id, for_level, amount, currency, status, payment_id)
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Ошибка при создании доната для перехода на уровень {for_level}: {e}")
                conn.rollback()
                return False

    def has_level_records(self, user_id: int, level: int) -> bool:
        """Проверяет, есть ли записи для указанного уровня"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT 1 FROM donations 
                    WHERE user_id = %s AND level = %s LIMIT 1""",
                    (user_id, level)
                )
                return cursor.fetchone() is not None
            except Exception as e:
                logger.error(f"Error checking level records in donations: {e}")
                return False

    def get_last_donation(self, user_id: int, level: int) -> Optional[dict]:
        """Получить последний донат пользователя для уровня"""
        with self.storage.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT * FROM donations 
                    WHERE user_id = %s AND level = %s 
                    ORDER BY donation_date DESC LIMIT 1""",
                    (user_id, level)
                )
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, result))
                return None

    def update_donation_status(self, donation_id: int, status: str,
                               payment_id: str = None, processed: bool = False) -> bool:
        """
        Обновляет статус доната с дополнительными проверками и обработкой успешных платежей

        Args:
            donation_id: ID записи о донате
            status: Новый статус (должен быть одним из предопределенных)
            payment_id: ID платежа в платежной системе (обязателен для успешных платежей)
            processed: Флаг обработки платежа

        Returns:
            bool: True если обновление прошло успешно, False в случае ошибки

        Raises:
            ValueError: При невалидном статусе или отсутствующем payment_id для успешных платежей
        """
        logger.info(f"Updating donation {donation_id} to status {status} with payment_id {payment_id}")

        # Валидация статуса
        valid_statuses = {
            self.STATUS_PENDING,
            self.STATUS_WAITING_FOR_CAPTURE,
            self.STATUS_SUCCEEDED,
            self.STATUS_CANCELED
        }
        if status not in valid_statuses:
            raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}")

        # Дополнительная проверка для успешных платежей
        if status == self.STATUS_SUCCEEDED and not payment_id:
            raise ValueError("payment_id is required for succeeded payments")

        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                # 1. Получаем текущие данные о донате
                cursor.execute(
                    "SELECT user_id, level, status FROM donations WHERE id = %s FOR UPDATE",
                    (donation_id,)
                )
                donation = cursor.fetchone()

                if not donation:
                    logger.error(f"Donation {donation_id} not found")
                    return False

                user_id, level, old_status = donation

                # 2. Проверяем переход статусов
                if old_status == self.STATUS_SUCCEEDED and status != self.STATUS_SUCCEEDED:
                    logger.warning(f"Attempt to change status from succeeded to {status}")
                    return False

                # 3. Обновляем запись
                update_query = """UPDATE donations 
                                SET status = %s, 
                                    payment_id = COALESCE(%s, payment_id),
                                    processed = %s,
                                    donation_date = CASE 
                                        WHEN %s = 'succeeded' AND donation_date IS NULL 
                                        THEN NOW() 
                                        ELSE donation_date 
                                    END
                                WHERE id = %s"""
                cursor.execute(
                    update_query,
                    (status, payment_id, processed, status, donation_id)
                )

                # 4. Для успешных платежей создаем задание
                if status == self.STATUS_SUCCEEDED:
                    cursor.execute("""
                        INSERT INTO tasks (
                            user_id, level, task_type, 
                            start_time, end_time, completed
                        ) VALUES (
                            %s, %s, 'donation', 
                            NOW(), NOW(), TRUE
                        ) ON CONFLICT DO NOTHING
                    """, (user_id, level))

                conn.commit()

                if cursor.rowcount > 0:
                    logger.info(f"Successfully updated donation {donation_id} to status {status}")
                    return True

                logger.warning(f"No rows affected for donation {donation_id}")
                return False

            except Exception as e:
                conn.rollback()
                logger.error(f"Error updating donation {donation_id}: {str(e)}", exc_info=True)
                return False

    def get_donation_by_payment_id(self, payment_id: str) -> Optional[dict]:
        """Найти донат по payment_id"""
        with self.storage.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT * FROM donations 
                    WHERE payment_id = %s 
                    ORDER BY donation_date DESC LIMIT 1""",
                    (payment_id,)
                )
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, result))
                return None

    def get_pending_payments(self) -> list[dict]:
        """Возвращает список платежей со статусом 'pending'."""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT id, user_id, level, payment_id 
                    FROM donations 
                    WHERE status = 'pending'
                """)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"Ошибка при получении платежей: {e}")
                return []

    def is_donation_processed(self, donation_id: int) -> bool:
        """Проверить, был ли донат уже обработан"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT processed FROM donations WHERE id = %s",
                    (donation_id,)
                )
                result = cursor.fetchone()
                return result[0] if result else False
            except Exception as e:
                logger.error(f"Error checking donation processed status: {e}")
                return False

    def mark_as_processed(self, donation_id: int) -> bool:
        """Пометить донат как обработанный"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "UPDATE donations SET processed = TRUE WHERE id = %s",
                    (donation_id,)
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Error marking donation as processed: {e}")
                conn.rollback()
                return False

    def get_charity_donations(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить все благотворительные пожертвования пользователя"""
        with self.storage.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT * FROM donations 
                    WHERE user_id = %s AND level = 0
                    ORDER BY donation_date DESC""",
                    (user_id,)
                )
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]