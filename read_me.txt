#bot_probubuzhdenie

## Используемые технологии

Python 3.8, Telebot, Yookassa
СУБД PostgreSQL (порт 5433)  

Логирование: logging + bot.log  

## Настройка проекта

Проект настраивается через переменные окружения, указанные в файле `.env`.

Пример файла `env.example.py`

| Ключ               | Назначение                          | По умолчанию       |
|--------------------|-------------------------------------|--------------------|
| BOT_TOKEN          | Токен Telegram бота                 | -                  |
| ADMIN_IDS          | ID администраторов                  | -                  |
| DB_HOST            | Хост БД                             | localhost          |
| DB_PORT            | Порт БД                             | 5433               |
| DB_NAME            | Имя БД                              | probuzhdenie       |
| DB_USER            | Пользователь БД                     | bot_user           |
| DB_PASSWORD        | Пароль БД                           | 5g                 |
| YOOKASSA_SHOP_ID   | ID магазина ЮKassa                  | -                  |
| YOOKASSA_SECRET_KEY| Секретный ключ ЮKassa               | -                  |

**Локальный разворот проекта:**

1. В директории проекта создать виртуальное окружение:
   ```bash
   python -m venv venv
2. Активировать виртуальное окружение:
 # Linux/macOS:source venv/bin/activate
# Windows: venv\Scripts\activate.
3.Установить зависимости:pip install -r requirements.txt
4.Настроить БД PostgreSQL (порт 5433):
sudo -u postgres psql -p 5433 -c "CREATE DATABASE probuzhdenie;"
sudo -u postgres psql -p 5433 -c "CREATE USER postgres WITH PASSWORD '5g';"
sudo -u postgres psql -p 5433 -c "GRANT ALL ON DATABASE probuzhdenie TO postgres;"
5.Создать .env и заполнить по примеру env_example.py/
6.Применить миграции:запустить файл storage.migrator.(важно)
7.Запустить python run_bot.py


Регистрация новых компонентов:
Новые модули добавлять в соответствующие директории:
Платежные системы: payments/
Админ-функции: admin/
Уровни игры: levels/