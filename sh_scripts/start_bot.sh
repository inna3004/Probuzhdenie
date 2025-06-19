#!/bin/bash
# ========================
# Запуск Telegram-бота
# ========================

# Конфигурация
BOT_DIR="/home/user/awakening_bot"  # Укажите реальный путь
LOG_FILE="$BOT_DIR/bot.log"
PYTHON_CMD="python3"  # Или "python" для старых систем

# --- Инициализация ---
cd "$BOT_DIR" || {
    echo "[ERROR] Не удалось перейти в $BOT_DIR"
    exit 1
}

# --- Настройка виртуального окружения ---
if [ ! -d "venv" ]; then
    echo "[INFO] Инициализация venv..."
    $PYTHON_CMD -m venv venv || {
        echo "[ERROR] Ошибка создания venv"
        exit 1
    }
    source venv/bin/activate
    pip install --upgrade pip wheel || {
        echo "[ERROR] Ошибка обновления pip"
        exit 1
    }
    pip install -r requirements.txt || {
        echo "[ERROR] Ошибка установки зависимостей"
        exit 1
    }
else
    source venv/bin/activate
fi

# --- Проверка дублирующих процессов ---
if pgrep -f "python main.py" > /dev/null; then
    echo "[WARN] Бот уже запущен (PID: $(pgrep -f "python main.py"))"
    exit 1
fi

# --- Запуск ---
echo "[INFO] Старт бота..."
nohup $PYTHON_CMD main.py > "$LOG_FILE" 2>&1 &
sleep 3  # Ожидание инициализации

# --- Валидация ---
if ! pgrep -f "python main.py" > /dev/null; then
    echo "[ERROR] Запуск не удался! Логи: $LOG_FILE"
    exit 1
fi

# --- Результат ---
echo "[SUCCESS] Бот активен"
echo "• PID: $(pgrep -f "python main.py")"
echo "• Логи: tail -f $LOG_FILE"