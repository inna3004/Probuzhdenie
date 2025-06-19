from yookassa import Configuration, Payment
from service.repository import DonationRepository
from storage.postgres_storage import PostgresStorage
import logging
from yookassa import Payment
from service.config import MAX_LEVEL
from settings import YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY
logger = logging.getLogger(__name__)

storage = PostgresStorage(
    dbname='probuzhdenie',
    user='postgres',
    password='5g',
    host='localhost',
    port='5433'
)
donation_repo = DonationRepository(storage)

Configuration.account_id = YOOKASSA_SHOP_ID
Configuration.secret_key = YOOKASSA_SECRET_KEY


def create_payment(user_id: int, current_level: int, amount: float = 500.00) -> str:
    """
    Создает платеж для перехода на следующий уровень
    """
    if current_level > MAX_LEVEL:
        raise ValueError(f"Уровень {current_level + 1} не существует (максимальный уровень: {MAX_LEVEL})")
    try:
        next_level = current_level + 1
        payment = Payment.create({
            "amount": {
                "value": f"{amount:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": "https://t.me/Sovmestimost_par_bot"
            },
            "description": f"Донат для перехода на уровень {next_level}",
            "metadata": {
                "user_id": user_id,
                "current_level": current_level,
                "target_level": next_level
            },
            "capture": True
        })

        donation_repo.create_donation(
            user_id=user_id,
            for_level=current_level,
            amount=amount,
            currency="RUB",
            status="pending",
            payment_id=payment.id
        )

        return payment.confirmation.confirmation_url
    except Exception as e:
        logger.error(f"Ошибка создания платежа: {str(e)}")
        raise


def check_payment_status(payment_id: str) -> dict:
    """
    Возвращает расширенную информацию о статусе платежа
    {
        'status': 'pending'|'succeeded'|...,
        'valid_for_level_up': bool,
        'metadata': dict
    }
    """
    try:
        payment = Payment.find_one(payment_id)
        return {
            'status': payment.status,
            'valid_for_level_up': payment.status == 'succeeded',
            'metadata': payment.metadata
        }
    except Exception as e:
        logger.error(f"Payment check error: {str(e)}")
        return {'status': 'error', 'valid_for_level_up': False}


def create_charity_payment(user_id: int, amount: float) -> str:
    try:
        payment = Payment.create({
            "amount": {
                "value": f"{amount:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": "https://t.me/Sovmestimost_par_bot"
            },
            "description": "Благотворительное пожертвование",
            "metadata": {
                "user_id": user_id,
                "for_level": 0,  # 0 - признак благотворительности
                "is_charity": True
            },
            "capture": True
        })

        donation_repo.create_donation(
            user_id=user_id,
            for_level=0,
            amount=amount,
            currency="RUB",
            status="pending",
            payment_id=payment.id
        )

        return payment.confirmation.confirmation_url
    except Exception as e:
        logger.error(f"Charity payment error: {str(e)}")
        raise
