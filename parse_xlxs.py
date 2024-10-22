import pandas as pd
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor


# Функция для извлечения информации из листа с заполнением N/A
def extract_ticket_info_with_na(df):
    ticket_info = {}

    # Извлекаем данные, заполняя N/A если значение отсутствует
    ticket_info['Passenger Name'] = df.iloc[1, 1] if pd.notna(df.iloc[1, 1]) else 'N/A'
    ticket_info['Flight Number'] = df.iloc[3, 0] if pd.notna(df.iloc[3, 0]) else 'N/A'
    ticket_info['Departure Date'] = df.iloc[7, 0] if pd.notna(df.iloc[7, 0]) else 'N/A'
    ticket_info['Departure Time'] = df.iloc[7, 2] if pd.notna(df.iloc[7, 2]) else 'N/A'
    ticket_info['From'] = df.iloc[3, 3] if pd.notna(df.iloc[3, 3]) else 'N/A'
    ticket_info['To'] = df.iloc[3, 7] if pd.notna(df.iloc[3, 7]) else 'N/A'
    ticket_info['PNR'] = df.iloc[11, 1] if pd.notna(df.iloc[11, 1]) else 'N/A'
    ticket_info['Ticket Number'] = df.iloc[11, 4] if pd.notna(df.iloc[11, 4]) else 'N/A'
    ticket_info['Seat'] = df.iloc[9, 6] if pd.notna(df.iloc[9, 6]) else 'N/A'
    ticket_info['Sequence'] = df.iloc[1, 6] if pd.notna(df.iloc[1, 6]) else 'N/A'
    ticket_info['Service Class'] = df.iloc[1, 7] if pd.notna(df.iloc[1, 7]) else 'N/A'

    return ticket_info


# Функция для обработки одного файла xlsx
def process_xlsx_file(file_path):
    sheets = pd.read_excel(file_path, sheet_name=None)
    file_ticket_info = []

    for sheet_name, sheet_df in sheets.items():
        ticket_info = extract_ticket_info_with_na(sheet_df)
        file_ticket_info.append(ticket_info)

    return file_ticket_info


# Путь к zip файлу и папке для распаковки
zip_path = 'C:/Users/shilo/PycharmProjects/pythonProject/data/YourBoardingPassDotAero.zip'  # Укажите путь к вашему zip-файлу
extract_path = 'C:/Users/shilo/PycharmProjects/pythonProject/data/extracted_files'  # Путь для извлечения файлов

# Проверяем, существует ли папка с уже распакованными файлами
if not os.path.exists(extract_path):
    os.makedirs(extract_path)
    # Извлечение файлов из zip-архива
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
else:
    print(f"Файлы уже распакованы в директорию {extract_path}")

# Получаем список всех файлов .xlsx в директории
xlsx_files = [os.path.join(extract_path, f) for f in os.listdir(extract_path) if f.endswith('.xlsx')]

# Обработка файлов в многопоточном режиме
all_ticket_info_with_na = []

# Используем ThreadPoolExecutor для многопоточной обработки
with ThreadPoolExecutor() as executor:
    results = executor.map(process_xlsx_file, xlsx_files)

# Сбор всех результатов из потоков
for result in results:
    all_ticket_info_with_na.extend(result)

# Создаем DataFrame из собранной информации
columns = ['Passenger Name', 'Flight Number', 'Departure Date', 'Departure Time', 'From', 'To', 'PNR',
           'Ticket Number', 'Seat', 'Sequence', 'Service Class']
tickets_with_na_df = pd.DataFrame(all_ticket_info_with_na, columns=columns)

# Сохраняем результат в .csv
output_csv_na_path = 'boarding_passes_with_na.csv'
tickets_with_na_df.to_csv(output_csv_na_path, index=False)

# Сохраняем результат в .tab
output_tab_na_path = 'boarding_passes_with_na.tab'
tickets_with_na_df.to_csv(output_tab_na_path, sep='\t', index=False)

print(f"Данные сохранены в {output_csv_na_path} и {output_tab_na_path}")
