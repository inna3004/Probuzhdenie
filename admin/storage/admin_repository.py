import logging
from typing import Dict, List, Optional
from datetime import datetime
from storage.postgres_storage import PostgresStorage
from settings import ADMIN_IDS

logger = logging.getLogger(__name__)


# Класс AdminRepository с методами для:
#     Проверки прав администратора
#     Получения статистики активных пользователей
#     Подсчета выполненных добрых дел
#     Статистики по уровням
#     Статистики донатов
#     Статистики рефералов

class AdminRepository:
    def __init__(self, storage: PostgresStorage):
        self.storage = storage

    def is_admin(self, user_id: int) -> bool:
        """Проверяет, является ли пользователь администратором"""
        return user_id in ADMIN_IDS

    def get_active_users_count(self) -> int:
        """Возвращает количество активных пользователей (играющих сейчас)"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT COUNT(*) FROM users 
                    WHERE current_level > 1 OR registration_complete = TRUE"""
                )
                return cursor.fetchone()[0] or 0
            except Exception as e:
                logger.error(f"Error getting active users count: {e}")
                return 0

    def get_completed_good_deeds_count(self) -> int:
        """Возвращает общее количество выполненных добрых дел"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT COUNT(*) FROM tasks 
                    WHERE completed = TRUE AND task_type != 'donation'"""
                )
                return cursor.fetchone()[0] or 0
            except Exception as e:
                logger.error(f"Error getting completed good deeds count: {e}")
                return 0

    def get_level_statistics(self) -> Dict[int, int]:
        """Возвращает статистику по уровням: сколько пользователей на каждом уровне"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT current_level, COUNT(*) FROM users 
                    WHERE registration_complete = TRUE
                    GROUP BY current_level"""
                )
                return {level: count for level, count in cursor.fetchall()}
            except Exception as e:
                logger.error(f"Error getting level statistics: {e}")
                return {}

    def get_donation_statistics(self) -> Dict[str, float]:
        """Возвращает статистику по донатам"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT 
                        COUNT(*) as count, 
                        SUM(amount) as total_amount 
                    FROM donations 
                    WHERE status = 'succeeded'"""
                )
                result = cursor.fetchone()
                return {
                    'total_count': result[0] or 0,
                    'total_amount': float(result[1] or 0)
                }
            except Exception as e:
                logger.error(f"Error getting donation statistics: {e}")
                return {'total_count': 0, 'total_amount': 0.0}

    def get_referral_statistics(self) -> Dict[str, int]:
        """Возвращает статистику по рефералам"""
        with self.storage.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM referrals")
                total = cursor.fetchone()[0] or 0

                cursor.execute(
                    """SELECT COUNT(*) FROM referrals 
                    WHERE registration_date IS NOT NULL"""
                )
                completed = cursor.fetchone()[0] or 0

                return {
                    'total_referrals': total,
                    'completed_referrals': completed,
                    'pending_referrals': total - completed
                }
            except Exception as e:
                logger.error(f"Error getting referral statistics: {e}")
                return {
                    'total_referrals': 0,
                    'completed_referrals': 0,
                    'pending_referrals': 0
                }
