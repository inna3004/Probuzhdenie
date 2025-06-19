import logging
from pathlib import Path
import threading
import time
from datetime import datetime, timedelta
import telebot
from telebot import types
from yookassa import Payment
from service.config import MAX_LEVEL
from admin.storage.admin_repository import AdminRepository
from payments.pay import create_payment, create_charity_payment
from service.repository import (
    UserRepository,
    UserDataRepository,
    LevelRepository,
    TaskRepository,
    ReferralRepository,
    DonationRepository
)
from service.states import BotStates
from settings import BOT_TOKEN
from storage.migrator import Migrator
from storage.postgres_storage import PostgresStorage
from logging.handlers import RotatingFileHandler

bot = telebot.TeleBot(BOT_TOKEN, threaded=True, num_threads=5)

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            filename='logs/bot.log',
            maxBytes=5 * 1024 * 1024,  # 5 MB на файл
            backupCount=3,  # 3 архивных копии
            encoding='utf-8'
        )
    ]
)

logger = logging.getLogger(__name__)

storage = PostgresStorage(
    dbname='probuzhdenie',
    user='postgres',
    password='5g',
    host='localhost',
    port='5433'
)

migrator = Migrator(storage)
migrator.migrate()

user_repo = UserRepository(storage)
user_data_repo = UserDataRepository(storage)
level_repo = LevelRepository(storage)
task_repo = TaskRepository(storage)
referral_repo = ReferralRepository(storage)
donation_repo = DonationRepository(storage)
admin_repo = AdminRepository(storage)
TASK_DURATION = timedelta(hours=24)


def create_main_menu_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(types.KeyboardButton("Правила игры"))
    keyboard.add(types.KeyboardButton("О боте"))
    return keyboard


def create_back_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(types.KeyboardButton("Назад"))
    return keyboard


def create_level_navigation_keyboard(current_level, user_id=None, task_repo=None):
    """Создает клавиатуру для навигации по уровням с кнопкой 'Далее'"""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    logger.info(f"[Keyboard] Creating navigation for level {current_level}")

    # Кнопка предыдущего уровня
    if current_level > 1:
        keyboard.add(types.KeyboardButton(f"{current_level - 1} уровень"))
        logger.info(f"[Keyboard] Added previous level button: {current_level - 1}")

    # Обработка кнопки "Далее" в зависимости от уровня
    if current_level == 1:
        # Для первого уровня - простая кнопка "Далее"
        keyboard.add(types.KeyboardButton("Далее"))
        logger.info("[Keyboard] Added simple 'Далее' button for level 1")

    elif current_level >= 21:
        # Для финального уровня - особая кнопка
        keyboard.add(types.KeyboardButton("Далее"))
        logger.info("[Keyboard] Added special 'Далее' button for final level")

    else:
        # Для промежуточных уровней (2-20)
        if task_repo is not None and user_id is not None:
            is_completed = task_repo.is_task_completed(user_id, current_level)
            logger.info(f"[Keyboard] Task completion check: {is_completed}")

            if is_completed:
                if current_level <= 20:
                    keyboard.add(types.KeyboardButton(f"{current_level + 1} уровень"))
                    logger.info(f"[Keyboard] Added next level button: {current_level + 1}")

        # Добавляем основную кнопку перехода с текстом
        keyboard.add(types.KeyboardButton("Далее, перейти к следующему уровню."))
        logger.info("[Keyboard] Added main 'Далее' button with text")

    # Кнопка правил для уровней 3-21
    if 2 <= current_level <= 21:
        keyboard.add(types.KeyboardButton("Правила игры для уровня игры:3-21"))
        logger.info("[Keyboard] Added special rules button")

    return keyboard


@bot.message_handler(func=lambda message: message.text == "Далее")
def handle_next_button(message):
    """Обработчик кнопки 'Далее' с учетом просматриваемого уровня"""
    try:
        user_id = message.from_user.id
        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)
        viewed_level = user_data_repo.get_viewed_level(user_id)

        logger.info(f"[Next Button] User {user_id} pressed 'Далее'. Current: {current_level}, Viewed: {viewed_level}")

        # Специальная обработка 1 уровня
        if viewed_level == 1:
            logger.info("[Next Button] Processing level 1 transition")
            task_repo.create_task(
                user_id=user_id,
                level=1,
                task_type='auto',
                start_time=datetime.now(),
                end_time=datetime.now(),
                completed=True
            )
            user_repo.update_user_level(user_id, 2)
            return show_level_content(message, 2)

        # Если пользователь просматривает уровень ниже текущего
        if viewed_level < current_level:
            logger.info(f"[Next Button] Viewing past level {viewed_level}, moving to {viewed_level + 1}")
            return show_level_content(message, viewed_level + 1)

        # Если достигнут максимальный уровень
        if viewed_level >= MAX_LEVEL:
            logger.info("[Next Button] Reached max level")
            return show_final_level_message(message)

        # Проверка выполнения задания для текущего уровня
        if not task_repo.is_task_completed(user_id, viewed_level):
            logger.info("[Next Button] Task not completed, showing selection")
            return show_task_selection(message)

        # Переход на следующий уровень
        next_level = viewed_level + 1
        logger.info(f"[Next Button] Moving from {viewed_level} to {next_level}")

        if not user_repo.update_user_level(user_id, next_level):
            logger.error("[Next Button] Failed to update user level")
            return bot.reply_to(message, "Ошибка обновления уровня")

        show_level_content(message, next_level)

    except Exception as e:
        logger.error(f"[Next Button] Error: {str(e)}", exc_info=True)
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Ссылка на сообщество" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.FINAL_LEVEL)
def handle_community_link(message):
    try:
        # Отправляем сообщение с кликабельной ссылкой
        bot.send_message(
            message.chat.id,
            "Нажмите на ссылку, чтобы присоединиться к нашему сообществу:\n"
            "https://t.me/your_community_link",  # Замените на реальную ссылку
            disable_web_page_preview=True
        )

        # Можно добавить кнопку "Назад" для удобства
        back_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        back_keyboard.add(types.KeyboardButton("Назад"))

        bot.send_message(
            message.chat.id,
            "После вступления вы можете вернуться в меню",
            reply_markup=back_keyboard
        )

    except Exception as e:
        logger.error(f"Error in handle_community_link: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


def show_final_level_message(message):
    try:
        final_text = (
            "🎉 Поздравляем! Вы успешно прошли все 21 уровень бота 'Пробуждение'!\n\n"
            "Вы достигли высшей ступени духовного развития в нашей системе.\n\n"
            "Теперь вы можете:\n"
            "1. Присоединиться к нашему закрытому сообществу для дальнейшего развития\n"
            "2. Поддержать проект благотворительным взносом\n\n"
            "Спасибо за ваше участие и преданность практике!"
        )

        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(types.KeyboardButton("Ссылка на сообщество"),
                     types.KeyboardButton("Благотворительность")
                     )

        bot.send_message(
            message.chat.id,
            final_text,
            reply_markup=keyboard
        )
        user_repo.set_user_state(message.from_user.id, BotStates.FINAL_LEVEL)

    except Exception as e:
        logger.error(f"Error in show_final_level_message: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


def show_level_content(message, level_number):
    """Показывает контент уровня и обновляет состояние просмотра"""
    try:
        user_id = message.from_user.id
        user_data_repo.set_viewed_level(user_id, level_number)
        user = user_repo.get_user(user_id)
        if user and user.get('current_level') < level_number:
            logger.warning(
                f"User {user_id} trying to view level {level_number} beyond current {user.get('current_level')}")
            level_number = user.get('current_level')
        level_content = level_repo.get_level_content(level_number)
        logger.info(f"[Level Content] Level content: {level_content[:50]}...")

        if not level_content:
            logger.warning(f"[Level Content] No content found for level {level_number}")
            bot.send_message(
                message.chat.id,
                "Контент для этого уровня пока недоступен."
            )
            return

        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)
        logger.info(f"[Level Content] User's current level in DB: {current_level}")

        is_completed = (task_repo.is_task_completed(user_id, current_level, 'time') or
                        task_repo.is_task_completed(user_id, current_level, 'referral') or
                        task_repo.is_task_completed(user_id, current_level, 'donation'))
        logger.info(f"[Level Content] Task completion status: {is_completed}")

        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)

        if level_number == 1:
            keyboard.add(
                types.KeyboardButton("Ответы на вопросы"),
                types.KeyboardButton("Далее")
            )
        else:
            keyboard = create_level_navigation_keyboard(
                level_number,
                user_id=user_id,
                task_repo=task_repo
            )
            logger.info(f"[Level Content] Created navigation keyboard for level {level_number}")

        bot.send_message(
            message.chat.id,
            level_content,
            reply_markup=keyboard
        )

        user_repo.set_user_state(user_id, BotStates.LEVEL_CONTENT)
        logger.info(f"[Level Content] User state set to LEVEL_CONTENT")

    except Exception as e:
        logger.error(f"[Level Content] Error in show_level_content: {str(e)}", exc_info=True)
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(commands=['admin'])
def handle_admin_command(message):
    try:
        user_id = message.from_user.id
        logger.info(f"Admin command received from {user_id}")

        if not admin_repo.is_admin(user_id):
            bot.reply_to(message, "⛔ У вас нет прав администратора")
            return

        # Получаем статистику
        active_users = admin_repo.get_active_users_count()
        good_deeds = admin_repo.get_completed_good_deeds_count()
        level_stats = admin_repo.get_level_statistics()
        donation_stats = admin_repo.get_donation_statistics()
        referral_stats = admin_repo.get_referral_statistics()

        # Формируем сообщение
        stats_message = (
            "📊 Статистика бота:\n\n"
            f"👥 Активных пользователей: {active_users}\n"
            f"🔄 Выполнено добрых дел: {good_deeds}\n\n"
            "📈 Статистика по уровням:\n"
        )

        for level, count in sorted(level_stats.items()):
            stats_message += f"  • Уровень {level}: {count} чел.\n"

        stats_message += (
            f"\n💸 Донаты: {donation_stats['total_count']} на сумму "
            f"{donation_stats['total_amount']:.2f} руб.\n"
            f"\n👥 Рефералы:\n"
            f"  • Всего приглашено: {referral_stats['total_referrals']}\n"
            f"  • Зарегистрировано: {referral_stats['completed_referrals']}\n"
            f"  • В процессе: {referral_stats['pending_referrals']}"
        )

        bot.reply_to(message, stats_message)

    except Exception as e:
        logger.error(f"Error in admin command: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка при получении статистики")


@bot.message_handler(func=lambda message: message.text == "Правила игры для уровня игры:3-21" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.LEVEL_CONTENT)
def handle_level_rules(message):
    try:
        rules_text = (
            "Правила платных уровней:\n\n"
            "Надо делать добрые дела либо платить за их неисполнение деньгами, "
            "тем самым Вселенная соблюдает баланс, который состоит из противостояния духа и материального."
        )

        bot.send_message(
            message.chat.id,
            rules_text,
            reply_markup=create_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in handle_level_rules: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


def validate_name(name):
    """Проверка корректности имени"""
    if not name or not name.strip():
        return False
    return name.isalpha()


def validate_birthdate(date_str):
    """Проверка корректности даты рождения"""
    try:
        datetime.strptime(date_str, '%d.%m.%Y')
        return True
    except ValueError:
        return False


@bot.message_handler(commands=['start'])
def handle_start(message):
    try:
        user_id = message.from_user.id
        referrer_id = None

        if len(message.text.split()) > 1 and message.text.split()[1].startswith('ref'):
            try:
                referrer_id = int(message.text.split()[1][3:])
                logger.info(f"Referral link detected. Referrer ID: {referrer_id}")

                if referrer_id != user_id:
                    # Сначала создаем пользователя, если его нет
                    user_repo.create_user(user_id)

                    # Затем пробуем создать реферальную запись
                    if not referral_repo.create_referral(referrer_id, user_id):
                        logger.error("Failed to create referral record")
            except (ValueError, IndexError) as e:
                logger.error(f"Error parsing referral link: {e}")

        user_repo.create_user(user_id)
        user = user_repo.get_user(user_id)

        if user and user.get('registration_complete'):
            show_main_menu(message)
        else:
            bot.send_message(
                message.chat.id,
                "Выберите язык:",
                reply_markup=types.ReplyKeyboardMarkup(
                    resize_keyboard=True,
                    one_time_keyboard=True
                ).add(types.KeyboardButton("Русский"))
            )
            user_repo.set_user_state(user_id, BotStates.LANGUAGE_SELECTION)
    except Exception as e:
        logger.error(f"Error in handle_start: {e}", exc_info=True)
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message:
user_repo.get_user_state(message.from_user.id) == BotStates.LANGUAGE_SELECTION)
def handle_language_selection(message):
    try:
        user_id = message.from_user.id
        logger.info(f"Language selection by user {user_id}: {message.text}")

        if message.text.lower() in ['русский', 'russian']:
            logger.info(f"Russian selected by {user_id}")

            if not user_data_repo.save_user_data(user_id=user_id, language='ru'):
                logger.error(f"Failed to save language for {user_id}")
                raise Exception("Language save failed")

            logger.info(f"Language saved for {user_id}")

            bot.send_message(
                message.chat.id,
                "Добро пожаловать в бота 'Пробуждение'!",
                reply_markup=create_main_menu_keyboard()
            )
            user_repo.set_user_state(user_id, BotStates.MAIN_MENU)
            logger.info(f"User {user_id} state set to MAIN_MENU")

        else:
            bot.send_message(
                message.chat.id,
                "Пожалуйста, выберите язык из предложенных вариантов."
            )
    except Exception as e:
        logger.error(f"Error in handle_language_selection for user {message.from_user.id}: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "О боте" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.MAIN_MENU)
def handle_about(message):
    try:
        about_text = (
            "Телеграм-бот предназначен для духовного сообщества «Создатели», "
            "цель которого – улучшить качество жизни каждого отдельного человека,"
            " в следствии чего – сделать мир лучше."
        )

        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(
            text="Сообщество 'Создатели'",
            url="https://example.com/community"
        ))

        bot.send_message(
            message.chat.id,
            about_text,
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Error in handle_about: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Правила игры" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.MAIN_MENU)
def handle_rules(message):
    logger.info(f"RAW MESSAGE CONTENT: {repr(message.text)}")
    user_id = message.from_user.id
    db_state = user_repo.get_user_state(user_id)
    expected_state = BotStates.MAIN_MENU
    logger.info(
        f"CRITICAL CHECK: db_state={db_state}, expected={expected_state}, types: {type(db_state)}/{type(expected_state)}")

    if db_state != expected_state:
        logger.error(f"STATE MISMATCH! Database returns: {db_state}")
        user = user_repo.get_user(user_id)
        logger.error(f"Full user data: {user}")
    if str(user_repo.get_user_state(message.from_user.id)) != str(BotStates.MAIN_MENU):
        logger.error("State validation failed!")
        return
    try:
        logger.info(f"Rules handler triggered for user {message.from_user.id}")

        rules_text = (
            "Правила игры:\n\n"
            "Совершать одно доброе дело в течение 21-го повторения (задания), чтобы закрепить привычку делать добро."
            "После каждого выполненного задания пользователь получает “добрые очки”, достижения и вдохновляющие истории"
        )

        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)

        user = user_repo.get_user(message.from_user.id)
        logger.info(f"User registration status: {user.get('registration_complete') if user else 'User not found'}")

        if user and user.get('registration_complete'):
            keyboard.add(types.KeyboardButton("Начать игру"))
        else:
            keyboard.add(types.KeyboardButton("Принять"))

        keyboard.add(types.KeyboardButton("Назад"))

        logger.info("Sending rules message with keyboard")
        bot.send_message(
            message.chat.id,
            rules_text,
            reply_markup=keyboard
        )
        logger.info("Rules message sent successfully")

    except Exception as e:
        logger.error(f"Error in handle_rules: {str(e)}", exc_info=True)
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


def start_registration(message):
    try:
        bot.send_message(
            message.chat.id,
            "Введите имя:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        user_repo.set_user_state(message.from_user.id, BotStates.REGISTRATION_NAME)
    except Exception as e:
        logger.error(f"Error in start_registration: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Принять" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.MAIN_MENU)
def handle_accept_rules(message):
    try:
        user_id = message.from_user.id
        logger.info(f"User {user_id} accepted the rules, starting registration")

        bot.send_message(
            message.chat.id,
            "Спасибо за принятие правил! Давайте начнем регистрацию.",
            reply_markup=types.ReplyKeyboardRemove()
        )

        bot.send_message(
            message.chat.id,
            "Введите ваше имя:",
            reply_markup=types.ReplyKeyboardRemove()
        )

        user_repo.set_user_state(user_id, BotStates.REGISTRATION_NAME)

    except Exception as e:
        logger.error(f"Error in handle_accept_rules: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message:
user_repo.get_user_state(message.from_user.id) == BotStates.REGISTRATION_NAME)
def process_name_step(message):
    try:
        if not validate_name(message.text):
            bot.send_message(
                message.chat.id,
                "Данные некорректны. Введите ваше реальное имя:"
            )
            return

        user_data_repo.save_user_data(
            user_id=message.from_user.id,
            name=message.text
        )

        bot.send_message(
            message.chat.id,
            "Введите дату рождения (в формате ДД.ММ.ГГГГ):"
        )
        user_repo.set_user_state(message.from_user.id, BotStates.REGISTRATION_BIRTHDATE)
    except Exception as e:
        logger.error(f"Error in process_name_step: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message:
user_repo.get_user_state(message.from_user.id) == BotStates.REGISTRATION_BIRTHDATE)
def process_birthdate_step(message):
    try:
        if not validate_birthdate(message.text):
            bot.send_message(
                message.chat.id,
                "Введите корректную дату (формат ДД.ММ.ГГГГ):"
            )
            return

        user_data_repo.save_user_data(
            user_id=message.from_user.id,
            birthdate=message.text
        )

        bot.send_message(
            message.chat.id,
            "Введите место проживания:"
        )
        user_repo.set_user_state(message.from_user.id, BotStates.REGISTRATION_LOCATION)
    except Exception as e:
        logger.error(f"Error in process_birthdate_step: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message:
user_repo.get_user_state(message.from_user.id) == BotStates.REGISTRATION_LOCATION)
def process_location_step(message):
    try:
        if not message.text or not message.text.strip():
            bot.send_message(
                message.chat.id,
                "Введите корректное место проживания:"
            )
            return

        user_data_repo.save_user_data(
            user_id=message.from_user.id,
            location=message.text
        )

        user_repo.complete_registration(message.from_user.id)

        bot.send_message(
            message.chat.id,
            "Регистрация завершена!",
            reply_markup=types.ReplyKeyboardMarkup(
                resize_keyboard=True
            ).add(types.KeyboardButton("Начать игру")))

        user_repo.set_user_state(message.from_user.id, BotStates.MAIN_MENU)
    except Exception as e:
        logger.error(f"Error in process_location_step: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Начать игру" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.MAIN_MENU)
def start_game(message):
    try:
        user = user_repo.get_user(message.from_user.id)
        current_level = user.get('current_level', 1)

        show_level_content(message, current_level)
    except Exception as e:
        logger.error(f"Error in start_game: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Ответы на вопросы" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.LEVEL_CONTENT)
def show_faq(message):
    try:
        faq_text = (
            "Часто задаваемые вопросы:\n\n"
            "1. Что такое душа человека?\n"
            "Душа — это нематериальная суть человека, которую многие считают источником сознания,"
            "чувств, совести и личности.\n\n"
            "2. Существует ли душа после смерти?\n"
            "Это вопрос веры и мировоззрения.\n\n"
            "3. Можно ли почувствовать душу?\n"
            "Многие люди описывают ощущение душевной теплоты, интуиции или внутреннего голоса\n\n "
            "4. Откуда берётся душа?\n"
            "Наука пока не может ответить на этот вопрос.\n\n"
            "5. Можно ли “испортить” или “очистить” душу?\n"
            "Да, в большинстве культур и религий душа может загрязниться плохими поступками, "
            "злобой, завистью, ложью.\n\n"

        )

        bot.send_message(
            message.chat.id,
            faq_text,
            reply_markup=create_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in show_faq: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Далее, перейти к следующему уровню." and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.LEVEL_CONTENT)
def handle_next_level_request(message):
    try:
        user_id = message.from_user.id
        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)

        if current_level >= MAX_LEVEL:
            logger.info(f"User {user_id} reached max level {MAX_LEVEL}, showing final message")
            return show_final_level_message(message)

        is_completed = task_repo.is_task_completed(user_id, current_level)
        logger.info(f"[Next Level] Task completion status for level {current_level}: {is_completed}")

        if is_completed:
            next_level = current_level + 1
            if next_level <= MAX_LEVEL:
                if not user_repo.update_user_level(user_id, next_level):
                    logger.error(f"Failed to update user {user_id} level to {next_level}")
                    return bot.reply_to(message, "Ошибка обновления уровня")

                logger.info(f"Successfully updated user {user_id} to level {next_level}")
                show_level_content(message, next_level)
            else:
                show_final_level_message(message)
        else:
            logger.info(f"Showing task selection for user {user_id} level {current_level}")
            show_task_selection(message)

    except Exception as e:
        logger.error(f"[Next Level] Error in handle_next_level_request: {str(e)}", exc_info=True)
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


def show_task_selection(message):
    try:
        user_id = message.from_user.id
        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)

        # Добавляем проверку и здесь
        if current_level >= MAX_LEVEL:
            logger.info(f"User {user_id} at max level in task selection, redirecting to final")
            return show_final_level_message(message)

        task_text = (
            "Для перехода на следующий уровень, требуется выполнить задание любое из приведённых заданий:"
            " отдать время, пригласить друга или сделать донат»\n\n"
        )

        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(
            types.KeyboardButton("Время"),
            types.KeyboardButton("Пригласи друга"),
            types.KeyboardButton("Донат"),
            types.KeyboardButton("Следующий уровень")
        )
        keyboard.add(types.KeyboardButton("Назад"))

        bot.send_message(
            message.chat.id,
            task_text,
            reply_markup=keyboard
        )

        user_repo.set_user_state(user_id, BotStates.TASK_SELECTION)
        logger.info(f"Set user {user_id} state to TASK_SELECTION")

    except Exception as e:
        logger.error(f"Error in show_task_selection: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Время" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.TASK_SELECTION)
def handle_time_task(message):
    try:
        user = user_repo.get_user(message.from_user.id)
        current_level = user.get('current_level', 1)

        active_task = task_repo.get_active_time_task(message.from_user.id, current_level)

        if active_task:
            end_time = active_task['start_time'] + TASK_DURATION
            time_left = end_time - datetime.now()

            if time_left.total_seconds() > 0:
                hours = int(time_left.total_seconds() // 3600)
                minutes = int((time_left.total_seconds() % 3600) // 60)

                task_text = (
                    f"Задание уже начато. Время на выполнение: {hours} часов {minutes} минут.\n\n"
                    f"Завершится: {end_time.strftime('%d.%m.%Y %H:%M')}"
                )

                keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
                keyboard.add(types.KeyboardButton("Задание выполнено"))
                keyboard.add(types.KeyboardButton("Назад"))

                bot.send_message(
                    message.chat.id,
                    task_text,
                    reply_markup=keyboard
                )

                user_repo.set_user_state(message.from_user.id, BotStates.TIME_TASK)
            else:
                task_repo.complete_task(
                    user_id=message.from_user.id,
                    level=current_level,
                    task_type='time'
                )

                bot.send_message(
                    message.chat.id,
                    "Задание выполнено! Теперь вы можете перейти на следующий уровень.",
                    reply_markup=create_level_navigation_keyboard(current_level)
                )

                user_repo.set_user_state(message.from_user.id, BotStates.LEVEL_CONTENT)
        else:
            task_text = (
                "Задание на практику:\n\n"
                "Выполняйте медитацию или другую практику в течение 24 часов.\n\n"
                "Начать задание?"
            )

            keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
            keyboard.add(
                types.KeyboardButton("Начать задание"),
                types.KeyboardButton("Назад")
            )

            bot.send_message(
                message.chat.id,
                task_text,
                reply_markup=keyboard
            )

            user_repo.set_user_state(message.from_user.id, BotStates.TIME_TASK)
    except Exception as e:
        logger.error(f"Error in handle_time_task: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Начать задание" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.TIME_TASK)
def start_time_task(message):
    try:
        user = user_repo.get_user(message.from_user.id)
        current_level = user.get('current_level', 1)

        task_repo.create_task(
            user_id=message.from_user.id,
            level=current_level,
            task_type='time',
            start_time=datetime.now(),
            end_time=datetime.now() + TASK_DURATION
        )

        end_time = datetime.now() + TASK_DURATION

        task_text = (
            "Задание начато!\n\n"
            f"Время на выполнение: 24 часа.\n"
            f"Завершится: {end_time.strftime('%d.%m.%Y %H:%M')}"
        )

        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(types.KeyboardButton("Задание выполнено"))
        keyboard.add(types.KeyboardButton("Назад"))

        bot.send_message(
            message.chat.id,
            task_text,
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Error in start_time_task: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Задание выполнено" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.TIME_TASK)
def complete_time_task(message):
    try:
        user_id = message.from_user.id
        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)

        active_task = task_repo.get_active_time_task(user_id, current_level)

        if not active_task:
            bot.send_message(
                message.chat.id,
                "У вас нет активных заданий.",
                reply_markup=create_back_keyboard()
            )
            return

        end_time = active_task['start_time'] + TASK_DURATION
        time_left = end_time - datetime.now()

        if time_left.total_seconds() > 0:
            hours = int(time_left.total_seconds() // 3600)
            minutes = int((time_left.total_seconds() % 3600) // 60)

            bot.send_message(
                message.chat.id,
                f"Время на выполнение задания ещё не вышло! Осталось: {hours} часов {minutes} минут.",
                reply_markup=create_back_keyboard()
            )
            return

        # 1. Отмечаем задание как выполненное
        task_repo.complete_task(
            user_id=user_id,
            level=current_level,
            task_type='time'
        )

        # 2. Обновляем уровень пользователя
        next_level = current_level + 1
        if next_level <= MAX_LEVEL:
            user_repo.update_user_level(user_id, next_level)

            # 3. Показываем контент нового уровня
            show_level_content(message, next_level)

            # 4. Отправляем уведомление о выполнении
            bot.send_message(
                message.chat.id,
                f"✅ Задание на время выполнено! Открыт {next_level} уровень",
                reply_markup=create_level_navigation_keyboard(
                    next_level,
                    user_id=user_id,
                    task_repo=task_repo
                )
            )
        else:
            # Обработка максимального уровня
            show_final_level_message(message)

        user_repo.set_user_state(user_id, BotStates.LEVEL_CONTENT)

    except Exception as e:
        logger.error(f"Error in complete_time_task: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Пригласи друга" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.TASK_SELECTION)
def handle_referral_task(message):
    try:
        user = user_repo.get_user(message.from_user.id)
        current_level = user.get('current_level', 1)

        referral_link = f"https://t.me/Sovmestimost_par_bot?start=ref{message.from_user.id}"

        referral_text = (
            "Пригласите друга в игру по этой ссылке:\n\n"
            f"{referral_link}\n\n"
            "После вступления в бота, откроется следующий уровень."
        )

        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(types.KeyboardButton("Проверить статус"))
        keyboard.add(types.KeyboardButton("Назад"))

        bot.send_message(
            message.chat.id,
            referral_text,
            reply_markup=keyboard
        )

        user_repo.set_user_state(message.from_user.id, BotStates.REFERRAL_TASK)
    except Exception as e:
        logger.error(f"Error in handle_referral_task: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


def check_referral_status(message):
    try:
        user_id = message.from_user.id
        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)

        # 1. Проверяем статус рефералов
        referral_status = referral_repo.get_referral_status(user_id, current_level)

        if referral_status['error']:
            bot.send_message(
                message.chat.id,
                "Произошла ошибка при проверке статуса рефералов. Пожалуйста, попробуйте позже."
            )
            return

        if referral_status['completed_referrals'] > 0:
            # 2. Создаем выполненное задание
            task_repo.create_task(
                user_id=user_id,
                level=current_level,
                task_type='referral',
                start_time=datetime.now(),
                end_time=datetime.now(),
                completed=True
            )

            # 3. Обновляем уровень пользователя
            next_level = current_level + 1
            if next_level <= MAX_LEVEL:
                user_repo.update_user_level(user_id, next_level)

                # 4. Показываем контент нового уровня
                show_level_content(message, next_level)

                # 5. Отправляем уведомление о выполнении
                bot.send_message(
                    message.chat.id,
                    f"✅ Реферальное задание выполнено! Приглашено: {referral_status['completed_referrals']}\n"
                    f"Открыт {next_level} уровень",
                    reply_markup=create_level_navigation_keyboard(
                        next_level,
                        user_id=user_id,
                        task_repo=task_repo
                    )
                )
            else:
                # Обработка максимального уровня
                show_final_level_message(message)

            user_repo.set_user_state(user_id, BotStates.LEVEL_CONTENT)
        else:
            # Оригинальная логика при незавершенном задании
            response = (
                "❌ Ваш друг ещё не зарегистрировался.\n"
                f"Всего приглашено: {referral_status['total_referrals']}\n"
                "Продолжайте приглашать друзей!"
            )

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
            markup.add(
                types.KeyboardButton("Проверить статус"),
                types.KeyboardButton("Назад")
            )

            bot.send_message(
                message.chat.id,
                response,
                reply_markup=markup
            )

    except Exception as e:
        logger.error(f"Error in check_referral_status: {str(e)}", exc_info=True)
        bot.reply_to(
            message,
            "Произошла непредвиденная ошибка при проверке статуса. "
            "Администратор уже уведомлен. Пожалуйста, попробуйте позже."
        )


@bot.message_handler(func=lambda message: message.text == "Донат" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.TASK_SELECTION)
def handle_donation_selection(message):
    try:
        user_id = message.from_user.id
        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)

        # Проверяем, что текущий уровень >= 2
        if current_level < 2:
            bot.send_message(
                message.chat.id,
                "Первый уровень не требует выполнения заданий. Используйте кнопку 'Далее'.",
                reply_markup=create_level_navigation_keyboard(current_level, user_id, task_repo)
            )
            return

        user_repo.set_user_state(message.from_user.id, BotStates.DONATION_TASK)

        if current_level >= MAX_LEVEL:
            bot.send_message(
                message.chat.id,
                "Поздравляем! Вы достигли максимального уровня.",
                reply_markup=create_level_navigation_keyboard(MAX_LEVEL)
            )
            return
        # Создаем донат для ТЕКУЩЕГО уровня
        payment_url = create_payment(message.from_user.id, current_level)

        if payment_url:
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton(
                text="Оплатить",
                url=payment_url
            ))

            bot.send_message(
                message.chat.id,
                f"Для перехода на следующий уровень ({current_level + 1}) сделайте донат:",
                reply_markup=keyboard
            )

            check_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
            check_keyboard.add(types.KeyboardButton("Проверить статус"))
            check_keyboard.add(types.KeyboardButton("Назад"))
            bot.send_message(
                message.chat.id,
                "После оплаты нажмите 'Проверить статус'",
                reply_markup=check_keyboard
            )
        else:
            bot.send_message(
                message.chat.id,
                "Ошибка при создании платежа. Попробуйте позже.",
                reply_markup=create_back_keyboard()
            )

    except Exception as e:
        logger.error(f"Error in handle_donation_selection: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Проверить статус" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.DONATION_TASK)
def check_donation_status(message):
    try:
        user_id = message.from_user.id
        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)
        logger.info(f"[Donation] Checking status for user {user_id}, level {current_level}")

        donation = donation_repo.get_last_donation(user_id, current_level)
        logger.info(f"[Donation] Last donation record: {donation}")

        if not donation:
            logger.warning(f"[Donation] No donation found for user {user_id}")
            bot.send_message(
                message.chat.id,
                "❌ Донат не найден. Пожалуйста, создайте новый платеж.",
                reply_markup=create_back_keyboard()
            )
            return

        payment_id = donation.get('payment_id')
        if not payment_id:
            logger.error(f"[Donation] No payment_id in donation record for user {user_id}")
            bot.send_message(
                message.chat.id,
                "❌ Ошибка: идентификатор платежа отсутствует. Создайте платеж заново.",
                reply_markup=create_back_keyboard()
            )
            return

        try:
            logger.info(f"[Donation] Fetching payment info for payment_id: {payment_id}")
            payment_info = Payment.find_one(payment_id)
            logger.info(f"[Donation] Payment status: {payment_info.status}")

            # Добавленная проверка статуса платежа
            if payment_info.status != 'succeeded':
                bot.send_message(
                    message.chat.id,
                    f"⚠️ Платеж еще не подтвержден. Текущий статус: {payment_info.status}",
                    reply_markup=create_back_keyboard()
                )
                return

        except Exception as e:
            logger.error(f"[Donation] Error getting payment info: {e}")
            bot.send_message(
                message.chat.id,
                "⚠️ Не удалось проверить статус платежа. Попробуйте позже.",
                reply_markup=create_back_keyboard()
            )
            return

        # Проверка на уже обработанный платеж
        if donation.get('processed', False):
            logger.info(f"[Donation] Donation already processed for user {user_id}")
            bot.send_message(
                message.chat.id,
                "ℹ️ Этот платеж уже был обработан ранее.",
                reply_markup=create_level_navigation_keyboard(
                    current_level + 1,
                    user_id=user_id,
                    task_repo=task_repo
                )
            )
            return

        # Обновление статуса доната
        donation_updated = donation_repo.update_donation_status(
            donation_id=donation['id'],
            status='succeeded',
            payment_id=payment_id,
            processed=True
        )
        logger.info(f"[Donation] Donation update result: {donation_updated}")

        # Создание задания
        if not task_repo.is_task_completed(user_id, current_level, 'donation'):
            logger.info(f"[Donation] Creating and completing donation task...")
            task_created = task_repo.create_task(
                user_id=user_id,
                level=current_level,
                task_type='donation',
                start_time=datetime.now(),
                end_time=datetime.now(),
                completed=True
            )
            if not task_created:
                logger.error(f"[Donation] Failed to create task for user {user_id}")
                bot.send_message(
                    message.chat.id,
                    "❌ Ошибка при создании задания. Обратитесь в поддержку.",
                    reply_markup=create_back_keyboard()
                )
                return

        # Обновление уровня пользователя
        next_level = current_level + 1
        if next_level <= MAX_LEVEL:
            if not user_repo.update_user_level(user_id, next_level):
                logger.error(f"[Donation] Failed to update user level to {next_level}")
                bot.send_message(
                    message.chat.id,
                    "⚠️ Ошибка обновления уровня. Обратитесь в поддержку.",
                    reply_markup=create_back_keyboard()
                )
                return

            logger.info(f"[Donation] User level updated to {next_level}")
            show_level_content(message, next_level)

            bot.send_message(
                message.chat.id,
                f"✅ Платеж успешно завершен! Теперь доступен {next_level} уровень.",
                reply_markup=create_level_navigation_keyboard(next_level, user_id, task_repo)
            )
        else:
            show_final_level_message(message)

        user_repo.set_user_state(user_id, BotStates.LEVEL_CONTENT)

    except Exception as e:
        logger.error(f"[Donation] Error in check_donation_status: {str(e)}", exc_info=True)
        bot.send_message(
            message.chat.id,
            "⚠️ Произошла внутренняя ошибка. Пожалуйста, попробуйте позже или обратитесь в поддержку.",
            reply_markup=create_back_keyboard()
        )


@bot.message_handler(func=lambda message: message.text == "Следующий уровень" and
                                          user_repo.get_user_state(message.from_user.id)
                                          in [BotStates.LEVEL_CONTENT, BotStates.TASK_SELECTION])
def handle_next_level_button(message):
    try:
        user_id = message.from_user.id
        logger.info(f"[Next Level] Button pressed by user {user_id}")

        user = user_repo.get_user(user_id)
        current_level = user.get('current_level', 1)
        logger.info(f"[Next Level] Current level: {current_level}")

        is_completed = task_repo.is_task_completed(user_id, current_level)
        logger.info(f"[Next Level] Task completed status: {is_completed}")

        if is_completed:
            next_level = current_level + 1
            logger.info(f"[Next Level] Preparing to move to level {next_level}")

            if next_level <= MAX_LEVEL:
                update_success = user_repo.update_user_level(user_id, next_level)
                logger.info(f"[Next Level] Level update result: {update_success}")

                if update_success:
                    show_level_content(message, next_level)
                else:
                    bot.reply_to(message, "Ошибка обновления уровня.")
            else:
                show_final_level_message(message)
        else:
            logger.info("[Next Level] Task not completed, showing details")
            show_task_status_details(message, current_level)

    except Exception as e:
        logger.error(f"[Next Level] Error: {str(e)}", exc_info=True)
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


def show_task_status_details(message, current_level):
    try:
        user_id = message.from_user.id
        status_messages = []

        active_time_task = task_repo.get_active_time_task(user_id, current_level)
        completed_time_task = task_repo.is_task_completed(user_id, current_level, 'time')

        if active_time_task:
            end_time = active_time_task['start_time'] + TASK_DURATION
            time_left = end_time - datetime.now()
            if time_left.total_seconds() > 0:
                hours = int(time_left.total_seconds() // 3600)
                minutes = int((time_left.total_seconds() % 3600) // 60)
                status_messages.append(f"⏳ Задание на время: осталось {hours}ч {minutes}мин")
        elif not completed_time_task:
            status_messages.append("⏱ Задание 'Время' не выполнено")

        referral_status = referral_repo.get_referral_status(user_id, current_level)
        completed_referral_task = task_repo.is_task_completed(user_id, current_level, 'referral')

        if not completed_referral_task:
            status_messages.append(f"👥 Зарегистрировано друзей: {referral_status['completed_referrals']}/{1}")

        last_donation = donation_repo.get_last_donation(user_id, current_level)
        completed_donation_task = task_repo.is_task_completed(user_id, current_level, 'donation')

        if not completed_donation_task:
            if last_donation and last_donation['status'] == 'pending':
                status_messages.append("💳 Донат: ожидает оплаты")
            else:
                status_messages.append("💳 Донат: не выполнен")

        response = (
                "Для перехода на следующий уровень выполните ЛЮБОЕ из заданий:\n\n" +
                "\n".join(status_messages) +
                "\n\nВыберите задание из меню ниже:"
        )

        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(
            types.KeyboardButton("Время"),
            types.KeyboardButton("Пригласи друга"),
            types.KeyboardButton("Донат")
        )
        keyboard.add(types.KeyboardButton("Назад"))

        bot.send_message(
            message.chat.id,
            response,
            reply_markup=keyboard
        )
        user_repo.set_user_state(user_id, BotStates.TASK_SELECTION)

    except Exception as e:
        logger.error(f"Error in show_task_status_details: {str(e)}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Назад")
def handle_back(message):
    """Обработчик кнопки 'Назад' с учетом просматриваемого уровня"""
    try:
        user_id = message.from_user.id
        viewed_level = user_data_repo.get_viewed_level(user_id)

        logger.info(f"[Back Button] User {user_id} pressed 'Назад'. Current viewed level: {viewed_level}")

        if viewed_level > 1:
            new_level = viewed_level - 1
            logger.info(f"[Back Button] Moving back to level {new_level}")
            show_level_content(message, new_level)
        else:
            logger.info("[Back Button] Already at level 1, showing main menu")
            show_main_menu(message)

    except Exception as e:
        logger.error(f"[Back Button] Error: {str(e)}", exc_info=True)
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


def show_main_menu(message):
    try:
        user_id = message.from_user.id
        logger.info(f"Showing main menu for user {user_id}")

        # Создаем клавиатуру с проверкой
        keyboard = create_main_menu_keyboard()
        if not keyboard or not isinstance(keyboard, types.ReplyKeyboardMarkup):
            logger.warning("Keyboard creation failed, using fallback")
            keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
            keyboard.add(types.KeyboardButton("Правила игры"))
            keyboard.add(types.KeyboardButton("О боте"))

        # Отправляем сообщение
        bot.send_message(
            message.chat.id,
            "🏠 Главное меню",
            reply_markup=keyboard
        )

        # Обновляем состояние
        if not user_repo.set_user_state(user_id, BotStates.MAIN_MENU):
            logger.error(f"Failed to update state for user {user_id}")

    except Exception as e:
        logger.error(f"Error in show_main_menu for user {user_id}: {str(e)}", exc_info=True)
        try:
            # Минимальный fallback
            bot.send_message(
                message.chat.id,
                "Добро пожаловать! Используйте кнопки меню.",
                reply_markup=types.ReplyKeyboardMarkup(
                    resize_keyboard=True
                ).add(types.KeyboardButton("Правила игры"))
            )
            user_repo.set_user_state(user_id, BotStates.MAIN_MENU)
        except:
            logger.critical("Complete failure in show_main_menu fallback")


@bot.message_handler(func=lambda message: "уровень" in message.text and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.LEVEL_CONTENT)
def handle_level_navigation(message):
    try:
        level_number = int(message.text.split()[0])
        show_level_content(message, level_number)
    except Exception as e:
        logger.error(f"Error in handle_level_navigation: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "Сообщество 'Создатели'" and
                                          user_repo.get_user_state(message.from_user.id) == BotStates.MAIN_MENU)
def handle_community_link(message):
    try:
        community_text = (
            "Добро пожаловать в наше закрытое сообщество 'Создатели'!\n\n"
            "Здесь вы найдете:\n"
            "- Ежедневные практики и медитации\n"
            "- Поддержку единомышленников\n"
            "- Эксклюзивные материалы\n"
            "- Личные консультации\n\n"
            "Присоединяйтесь по ссылке ниже:"
        )

        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(
            text="Присоединиться к сообществу",
            url="https://t.me/your_community_link"  # Замените на реальную ссылку
        ))

        bot.send_message(
            message.chat.id,
            community_text,
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Error in handle_community_link: {e}")
        bot.reply_to(message, "Произошла ошибка. Пожалуйста, попробуйте позже.")


# Состояние для ввода суммы
CHARITY_AMOUNT_INPUT = 100


@bot.message_handler(func=lambda message: message.text == "Благотворительность")
def handle_charity(message):
    msg = bot.send_message(
        message.chat.id,
        "Введите сумму благотворительного пожертвования (в рублях):",
        reply_markup=types.ReplyKeyboardRemove()
    )
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(
        types.KeyboardButton("Назад"),
    )
    bot.register_next_step_handler(msg, process_charity_amount)


def process_charity_amount(message):
    try:
        amount = float(message.text.replace(',', '.'))
        if amount < 1:
            bot.send_message(message.chat.id, "Минимальная сумма - 1 рубль")
            return

        user_repo.set_user_state(message.from_user.id, CHARITY_AMOUNT_INPUT)

        payment_url = create_charity_payment(
            user_id=message.from_user.id,
            amount=amount
        )

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(
            text="Оплатить",
            url=payment_url
        ))

        reply_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        reply_markup.add(types.KeyboardButton("Проверить статус пожертвования"))

        bot.send_message(
            message.chat.id,
            f"Сумма пожертвования: {amount} руб.\nНажмите кнопку ниже для оплаты:",
            reply_markup=markup
        )
        bot.send_message(
            message.chat.id,
            "После оплаты нажмите 'Проверить статус пожертвования'",
            reply_markup=reply_markup
        )

    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите корректную сумму")
    except Exception as e:
        logger.error(f"Charity error: {str(e)}")
        bot.send_message(message.chat.id, "Произошла ошибка, попробуйте позже")


@bot.message_handler(func=lambda message: message.text == "Проверить статус пожертвования")
def check_charity_status(message):
    try:
        user_id = message.from_user.id
        donation = donation_repo.get_last_donation(user_id, level=0)

        if not donation:
            bot.send_message(message.chat.id, "Пожертвование не найдено",
                             reply_markup=create_back_keyboard())
            return

        payment_id = donation['payment_id']
        payment = Payment.find_one(payment_id)

        if payment.status == 'succeeded':
            donation_repo.update_donation_status(
                donation_id=donation['id'],
                status='succeeded',
                payment_id=payment_id
            )

            # Используем стандартное главное меню вместо кастомной клавиатуры
            user_repo.set_user_state(user_id, BotStates.MAIN_MENU)
            show_main_menu(message)  # Используем существующую функцию для показа меню

            bot.send_message(
                message.chat.id,
                "✅ Пожертвование успешно получено! Спасибо за вашу поддержку!"
            )
        elif payment.status == 'pending':
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
            markup.add(types.KeyboardButton("Проверить статус пожертвования"))
            bot.send_message(
                message.chat.id,
                "⏳ Платеж ожидает оплаты",
                reply_markup=markup
            )
        else:
            bot.send_message(
                message.chat.id,
                f"Статус платежа: {payment.status}",
                reply_markup=create_back_keyboard()
            )

    except Exception as e:
        logger.error(f"Charity status check error: {str(e)}")
        bot.send_message(
            message.chat.id,
            "Ошибка при проверке статуса",
            reply_markup=create_back_keyboard()
        )


@bot.message_handler(func=lambda message: True)
def debug_all_messages(message):
    user_state = user_repo.get_user_state(message.from_user.id)
    logger.info(f"DEBUG: Получено сообщение '{message.text}' | Текущее состояние: {user_state}")

    if message.text == "Правила игры":
        logger.info("DEBUG: Обнаружено сообщение 'Правила игры'!")
        handle_rules(message)


def payment_poller():
    """Фоновая задача: проверяет платежи каждые 60 секунд."""
    while True:
        try:
            pending_payments = donation_repo.get_pending_payments()
            processed_users = set()

            for payment in pending_payments:
                if payment['user_id'] in processed_users:
                    continue

                if donation_repo.is_donation_processed(payment['id']):
                    continue

                try:
                    payment_info = Payment.find_one(payment['payment_id'])

                    if payment_info.status == 'succeeded':
                        donation_repo.update_donation_status(
                            donation_id=payment['id'],
                            status='succeeded',
                            payment_id=payment['payment_id'],
                            processed=True
                        )

                        if payment['level'] == 0:  # Благотворительность
                            try:
                                bot.send_message(
                                    payment['user_id'],
                                    "✅ Пожертвование успешно получено! Спасибо за вашу поддержку!",
                                    reply_markup=create_main_menu_keyboard()
                                )
                                user_repo.set_user_state(payment['user_id'], BotStates.MAIN_MENU)
                            except Exception as e:
                                logger.error(f"Can't notify user {payment['user_id']}: {e}")
                            processed_users.add(payment['user_id'])
                            continue

                        # Для обычных платежей
                        task_repo.create_task(
                            user_id=payment['user_id'],
                            level=payment['level'],
                            task_type='donation',
                            start_time=datetime.now(),
                            end_time=datetime.now(),
                            completed=True
                        )

                        next_level = payment['level'] + 1
                        user_repo.update_user_level(payment['user_id'], next_level)
                        user_repo.set_user_state(payment['user_id'], BotStates.LEVEL_CONTENT)

                        level_content = level_repo.get_level_content(next_level)
                        keyboard = create_level_navigation_keyboard(
                            next_level,
                            user_id=payment['user_id'],
                            task_repo=task_repo
                        )

                        if level_content:
                            bot.send_message(
                                payment['user_id'],
                                level_content,
                                reply_markup=keyboard
                            )

                        bot.send_message(
                            payment['user_id'],
                            f"✅ Платеж подтвержден! Теперь доступен {next_level} уровень.",
                            reply_markup=keyboard
                        )

                        processed_users.add(payment['user_id'])

                    elif payment_info.status == 'canceled':
                        donation_repo.update_donation_status(
                            donation_id=payment['id'],
                            status='canceled',
                            payment_id=payment['payment_id']
                        )

                except Exception as e:
                    logger.error(f"Error processing payment {payment['payment_id']}: {e}")

        except Exception as e:
            logger.error(f"Payment poller error: {e}")

        time.sleep(60)

def run_bot():
    logger.info("Starting bot...")
    logger.info("Testing database connection...")
    if not storage.test_connection():
        logger.error("Database connection failed")
        exit(1)
    logger.info("Database connection successful")

    payment_thread = threading.Thread(
        target=payment_poller,
        name="PaymentPoller",
        daemon=True
    )
    payment_thread.start()
    logger.info("Background payment poller started")

    while True:
        try:
            logger.info("Starting bot polling...")
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"Bot crashed: {str(e)}", exc_info=True)
            time.sleep(5)
            continue
        finally:
            logger.info("Cleaning up resources...")
            storage.close()
            logger.info("Storage connection closed")
