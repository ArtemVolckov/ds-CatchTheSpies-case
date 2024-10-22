import pandas as pd
import numpy as np  # Импортируем numpy для работы с массивами
from multiprocessing import Pool


# Функция для обработки дубликатов пассажиров с разными PNR
def find_suspicious_by_pnr(data):
    suspicious_pnr = data.groupby('Passenger Name')['PNR'].nunique()
    suspicious_passengers_pnr = suspicious_pnr[suspicious_pnr > 1].index
    return data[data['Passenger Name'].isin(suspicious_passengers_pnr)]


# Функция для нахождения пассажиров с отсутствующими важными данными
def find_missing_data_suspicious(data):
    return data[data['Ticket Number'].isna()]


# Функция для анализа частоты полетов
def find_frequent_flyers(data):
    data['Departure Date'] = pd.to_datetime(data['Departure Date'])
    frequent_flyers = data.groupby('Passenger Name').size()
    frequent_flyers = frequent_flyers[frequent_flyers > 3].index  # Условие частоты полетов
    return data[data['Passenger Name'].isin(frequent_flyers)]


# Разделение данных на батчи
def split_data(data, n_splits):
    return np.array_split(data, n_splits)


# Основная функция для выполнения всех шагов в многопроцессорном режиме
def analyze_suspicious_passengers(file_path):
    # Чтение данных
    data = pd.read_csv(file_path, sep='\t')

    # Предобработка данных: преобразование дат
    data['Departure Date'] = pd.to_datetime(data['Departure Date'])

    # Разбиваем данные на части для параллельной обработки
    data_batches = split_data(data, 4)

    # Используем многопроцессорность для выполнения анализа
    with Pool() as pool:
        results_pnr = pool.map(find_suspicious_by_pnr, data_batches)
        results_missing = pool.map(find_missing_data_suspicious, data_batches)
        results_frequent = pool.map(find_frequent_flyers, data_batches)

        # Объединение всех результатов и удаление дубликатов
        suspicious_data_combined = pd.concat(results_pnr + results_missing + results_frequent).drop_duplicates()

    # Возвращаем результат
    return suspicious_data_combined


# Основной запуск программы
if __name__ == "__main__":
    # Путь к файлу
    file_path = 'C:/Users/shilo/PycharmProjects/pythonProject/data/boarding_passes_with_na.tab'

    # Выполнение анализа
    # Выполнение анализа
    suspicious_passengers = analyze_suspicious_passengers(file_path)

    # Сохранение результатов в CSV файл
    suspicious_passengers.to_csv('suspicious_passengers.csv', index=False)

    # Либо вывести первые несколько строк для проверки
    print(suspicious_passengers.head())
