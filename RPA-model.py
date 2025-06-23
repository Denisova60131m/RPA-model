import pandas as pd
import os
from datetime import datetime
import logging
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


class FileReconciliationRPA:
    def __init__(self):
        self.report_data = {
            "Этап": [],
            "Статус": [],
            "Описание": [],
            "Количество записей": [],
            "Время выполнения (сек)": []
        }
        self.error_data = {
            "Тип ошибки": [],
            "Описание": [],
            "Рекомендации": []
        }
        self.missing_requests = pd.DataFrame(columns=['ID из ZDPM', 'Тип записи', 'Статус в ZDPM'])
        self.total_records_checked = 0
        self.start_time = None

    def log_step(self, stage, status, description, count="", exec_time=None):
        """Логирование этапа процесса"""
        self.report_data["Этап"].append(stage)
        self.report_data["Статус"].append(status)
        self.report_data["Описание"].append(description)
        self.report_data["Количество записей"].append(count)
        self.report_data["Время выполнения (сек)"].append(exec_time)

        print(f"[{stage}] {status}: {description}")
        if count:
            print(f"Количество записей: {count}")
        if exec_time is not None:
            print(f"Время выполнения: {exec_time:.2f} сек")
        print("-" * 50)

    def log_error(self, error_type, description, recommendation):
        """Логирование ошибок"""
        self.error_data["Тип ошибки"].append(error_type)
        self.error_data["Описание"].append(description)
        self.error_data["Рекомендации"].append(recommendation)
        logger.error(f"{error_type}: {description}")

    def find_files(self, date_str):
        """Поиск файлов по дате"""
        start_time = time.time()
        try:
            zdpm_file = f"export_zdpm_{date_str}.xlsx"
            dmdm_file = f"requests_dmdm_{date_str}.xlsx"

            if not os.path.exists(zdpm_file):
                raise FileNotFoundError(f"Файл {zdpm_file} не найден")
            if not os.path.exists(dmdm_file):
                raise FileNotFoundError(f"Файл {dmdm_file} не найден")

            exec_time = time.time() - start_time
            self.log_step("Поиск файлов", "Успешно",
                          f"Найдены файлы для даты {date_str}: {zdpm_file}, {dmdm_file}",
                          exec_time=exec_time)
            return zdpm_file, dmdm_file

        except FileNotFoundError as e:
            exec_time = time.time() - start_time
            self.log_error("Ошибка поиска файлов", str(e),
                           "Проверьте наличие файлов в указанной директории")
            self.log_step("Поиск файлов", "Ошибка",
                          f"Файлы для даты {date_str} не найдены",
                          exec_time=exec_time)
            return None, None

    def read_and_count(self, file_path, system_name):
        """Чтение файла и подсчет записей"""
        start_time = time.time()
        try:
            df = pd.read_excel(file_path)
            count = len(df)

            if "zdpm" in file_path.lower():
                self.total_records_checked = count
                self.zdpm_df = df  # Сохраняем DataFrame для последующего сравнения

            exec_time = time.time() - start_time
            self.log_step(f"Чтение {system_name}", "Успешно",
                          f"Файл {file_path} успешно прочитан", count,
                          exec_time=exec_time)
            return count, df if "dmdm" in file_path.lower() else None

        except Exception as e:
            exec_time = time.time() - start_time
            self.log_error("Ошибка чтения файла",
                           f"Ошибка при чтении файла {file_path}: {str(e)}",
                           "Проверьте формат и содержимое файла")
            self.log_step(f"Чтение {system_name}", "Ошибка",
                          f"Ошибка при чтении файла {file_path}",
                          exec_time=exec_time)
            return None, None

    def compare_data(self, zdpm_df, dmdm_df):
        """Сравнение данных и поиск отсутствующих запросов"""
        start_time = time.time()

        # Получаем уникальные ID из ZDPM
        zdpm_ids = set(zdpm_df['ID'].unique())

        # Получаем SourceID из dMDM (должны соответствовать ID из ZDPM)
        dmdm_source_ids = set(dmdm_df['SourceID'].unique())

        # Находим отсутствующие запросы
        missing_ids = zdpm_ids - dmdm_source_ids

        # Заполняем DataFrame с отсутствующими запросами
        if missing_ids:
            missing_records = zdpm_df[zdpm_df['ID'].isin(missing_ids)]
            self.missing_requests = missing_records[['ID', 'Type', 'Status']].rename(
                columns={'ID': 'ID из ZDPM', 'Type': 'Тип записи', 'Status': 'Статус в ZDPM'})

        exec_time = time.time() - start_time
        return len(missing_ids), exec_time

    def compare_counts(self, count_zdpm, count_dmdm, zdpm_df, dmdm_df):
        """Сравнение количества записей и содержания"""
        start_time = time.time()

        if count_zdpm is None or count_dmdm is None:
            exec_time = time.time() - start_time
            self.log_step("Сравнение", "Пропущено",
                          "Сравнение не выполнено из-за ошибок чтения файлов",
                          exec_time=exec_time)
            return False

        # Сравниваем количество записей
        count_match = count_zdpm == count_dmdm

        # Сравниваем содержимое файлов
        missing_count, compare_time = self.compare_data(zdpm_df, dmdm_df)

        if count_match and missing_count == 0:
            exec_time = time.time() - start_time
            self.log_step("Сравнение", "Успешно",
                          "Полное соответствие количества записей и содержания",
                          f"ZDPM: {count_zdpm}, dMDM: {count_dmdm}",
                          exec_time=exec_time)
            return True
        else:
            message = []
            if not count_match:
                message.append(f"Несоответствие количества (ZDPM: {count_zdpm}, dMDM: {count_dmdm})")
            if missing_count > 0:
                message.append(f"Найдено {missing_count} отсутствующих запросов в dMDM")

            exec_time = time.time() - start_time
            self.log_step("Сравнение", "Расхождение",
                          "; ".join(message),
                          f"ZDPM: {count_zdpm}, dMDM: {count_dmdm}, Отсутствует: {missing_count}",
                          exec_time=exec_time)
            return False

    def generate_report(self, date_str):
        """Генерация отчета"""
        start_time = time.time()
        report_df = pd.DataFrame(self.report_data)
        error_df = pd.DataFrame(self.error_data)

        report_file = f"reconciliation_report_{date_str}.xlsx"

        with pd.ExcelWriter(report_file) as writer:
            report_df.to_excel(writer, sheet_name='Отчет', index=False)

            if not self.missing_requests.empty:
                self.missing_requests.to_excel(
                    writer, sheet_name='Отсутствующие запросы', index=False)

            if not error_df.empty:
                error_df.to_excel(writer, sheet_name='Ошибки', index=False)

        exec_time = time.time() - start_time
        self.log_step("Формирование отчета", "Завершено",
                      f"Отчет сохранен в файл {report_file}",
                      exec_time=exec_time)

    def run_reconciliation(self, date_str):
        """Основной метод выполнения сверки"""
        self.start_time = time.time()
        self.log_step("Инициализация", "Начато",
                      f"Запуск процесса сверки на дату {date_str}")

        # 1. Поиск файлов
        zdpm_file, dmdm_file = self.find_files(date_str)
        if zdpm_file is None or dmdm_file is None:
            self.generate_report(date_str)
            return False

        # 2. Чтение и подсчет ZDPM
        count_zdpm, _ = self.read_and_count(zdpm_file, "ZDPM")

        # 3. Чтение и подсчет dMDM
        count_dmdm, dmdm_df = self.read_and_count(dmdm_file, "dMDM")

        # 4. Сравнение
        self.compare_counts(count_zdpm, count_dmdm, self.zdpm_df, dmdm_df)

        # 5. Формирование отчета
        self.generate_report(date_str)

        total_time = time.time() - self.start_time
        self.log_step("Процесс сверки", "Завершен",
                      f"Сверка на дату {date_str} завершена. Проверено записей: {self.total_records_checked}",
                      exec_time=total_time)
        return True


def get_input_date():
    """Запрос даты у пользователя"""
    while True:
        date_str = input("Введите дату для сверки файлов в формате ДДММГГГГ: ")
        try:
            datetime.strptime(date_str, "%d%m%Y")
            return date_str
        except ValueError:
            print("Некорректный формат даты. Попробуйте снова.")


def main():
    print("=== RPA-сценарий для сверки файлов миграции данных между ZDPM и dMDM ===")
    date_str = get_input_date()

    rpa = FileReconciliationRPA()
    rpa.run_reconciliation(date_str)

    print("\n=== Итоговый статус выполнения ===")
    for stage, status, desc, count, exec_time in zip(
            rpa.report_data["Этап"],
            rpa.report_data["Статус"],
            rpa.report_data["Описание"],
            rpa.report_data["Количество записей"],
            rpa.report_data["Время выполнения (сек)"]
    ):
        print(f"{stage}: {status} - {desc}")
        if count:
            print(f"  Количество записей: {count}")
        if exec_time:
            print(f"  Время выполнения: {exec_time:.2f} сек")


if __name__ == "__main__":
    main()