#!/bin/bash
# ========================
# Остановка бота
# ========================

BOT_NAME="main.py"

echo "[INFO] Поиск активных процессов..."
PID=$(pgrep -f "python $BOT_NAME")

if [ -z "$PID" ]; then
    echo "[WARN] Процессы не найдены"
    exit 0
fi

echo "[INFO] Остановка (PID: $PID)..."
kill -9 $PID
sleep 2  # Ожидание завершения

if pgrep -f "python $BOT_NAME" > /dev/null; then
    echo "[ERROR] Не удалось остановить"
    exit 1
else
    echo "[SUCCESS] Бот остановлен"
fi