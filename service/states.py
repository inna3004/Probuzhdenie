# Класс BotStates
# Содержит константы состояний бота:
#     START - начальное состояние
#     LANGUAGE_SELECTION - выбор языка
#     MAIN_MENU - главное меню
#     REGISTRATION_NAME - ввод имени при регистрации
#     REGISTRATION_BIRTHDATE - ввод даты рождения
#     REGISTRATION_LOCATION - ввод местоположения
#     LEVEL_CONTENT - просмотр контента уровня
#     TASK_SELECTION - выбор задания
#     TIME_TASK - задание на время
#     REFERRAL_TASK - реферальное задание
#     DONATION_TASK - задание с донатом
class BotStates:
    START = 0
    LANGUAGE_SELECTION = 1
    MAIN_MENU = 2
    REGISTRATION_NAME = 3
    REGISTRATION_BIRTHDATE = 4
    REGISTRATION_LOCATION = 5
    LEVEL_CONTENT = 6
    TASK_SELECTION = 7
    TIME_TASK = 8
    REFERRAL_TASK = 9
    DONATION_TASK = 10
    FINAL_LEVEL = 11
    CHARITY_AMOUNT_INPUT = 12
    FAQ = 13