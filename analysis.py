import os
import pandas as pd

# Папка с файлами
folder = "data"

# Ищем все нужные CSV
transaction_files = [os.path.join(folder, f) for f in os.listdir(folder) if "transactions" in f and f.endswith(".csv")]
transfer_files = [os.path.join(folder, f) for f in os.listdir(folder) if "transfers" in f and f.endswith(".csv")]

# === Загружаем данные ===
transactions = pd.concat([pd.read_csv(f) for f in transaction_files], ignore_index=True)
transfers = pd.concat([pd.read_csv(f) for f in transfer_files], ignore_index=True)

# === 1. Анализ транзакций ===
# Сумма расходов по категориям
transactions_grouped = transactions.groupby(["client_code", "category"])["amount"].sum().unstack(fill_value=0)

# Количество операций по категориям
transactions_counts = transactions.groupby(["client_code", "category"])["amount"].count().unstack(fill_value=0)

# Переименовываем колонки
transactions_grouped = transactions_grouped.add_prefix("sum_")
transactions_counts = transactions_counts.add_prefix("count_")

transactions_final = transactions_grouped.join(transactions_counts)

# === 2. Анализ переводов ===
transfers_grouped = transfers.groupby(["client_code", "direction"])["amount"].sum().unstack(fill_value=0)
transfers_grouped = transfers_grouped.rename(columns={"in": "total_in", "out": "total_out"})

# === 3. Итоговая таблица ===
final = transactions_final.join(transfers_grouped, how="outer").reset_index()

# Сохраняем результат
final.to_csv("final_dataset.csv", index=False, encoding="utf-8-sig")

print("✅ Итоговый датасет сохранён в final_dataset.csv")
print("Размер таблицы:", final.shape)
