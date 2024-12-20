import pandas as pd
import pg8000
import logging

class WeatherDatabaseManager:
    """
    Класс для взаимодействия с базой данных PostgreSQL для хранения и получения погодных данных.
    """

    def __init__(self, db_name, user, password, host='localhost', port=5432):
        """
        Инициализация соединения с базой данных.

        :param db_name: Имя базы данных.
        :param user: Имя пользователя базы данных.
        :param password: Пароль пользователя базы данных.
        :param host: Хост базы данных. По умолчанию localhost.
        :param port: Порт базы данных. По умолчанию 5432.
        """
        self.connection = None
        try:
            self.connection = pg8000.connect(
                database=db_name,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.logger = logging.getLogger('WeatherDatabaseManager')
            self.logger.info("Соединение с базой данных установлено.")
        except Exception as e:
            logging.error(f"Ошибка подключения к базе данных: {e}")

    def create_table(self):
        """
        Создает таблицу для хранения погодных данных, если она не существует.
        """
        query = """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            station_id VARCHAR(50),
            timestamp TIMESTAMP,
            temp_avg FLOAT,
            temp_diff FLOAT,
            autocorr FLOAT,
            max_temp FLOAT,
            min_temp FLOAT
        );
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
                self.logger.info("Таблица weather_data успешно создана (или уже существует).")
        except Exception as e:
            self.logger.error(f"Ошибка при создании таблицы: {e}")

    def insert_data(self, data):
        """
        Вставляет данные анализа в таблицу weather_data.
        Проверяет наличие дубликатов перед вставкой.

        :param data: Список словарей с данными анализа.
        """
        try:
            with self.connection.cursor() as cursor:
                for entry in data:
                    # Проверка существования записи
                    cursor.execute(
                        "SELECT 1 FROM weather_data WHERE station_id = %s AND timestamp = %s",
                        (entry['station_id'], entry['timestamp'])
                    )
                    if not cursor.fetchone():
                        # Если записи нет, вставляем
                        cursor.execute(
                            """
                            INSERT INTO weather_data (station_id, timestamp, temp_avg, temp_diff, autocorr, max_temp, min_temp)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                entry['station_id'],
                                entry['timestamp'],
                                entry['temp_avg'],
                                entry['temp_diff'],
                                entry['autocorr'],
                                entry['max_temp'],
                                entry['min_temp']
                            )
                        )
                self.connection.commit()
                self.logger.info("Данные успешно вставлены в таблицу weather_data.")
        except Exception as e:
            self.logger.error(f"Ошибка при вставке данных: {e}")


    def fetch_data(self, station_id):
        """
        Извлекает данные из таблицы для указанной станции и возвращает их как DataFrame.

        :param station_id: Идентификатор станции.
        :return: DataFrame с данными.
        """
        query = "SELECT station_id, timestamp, temp_avg, temp_diff, autocorr, max_temp, min_temp FROM weather_data WHERE station_id = %s"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (station_id,))
                records = cursor.fetchall()
                columns = ['station_id', 'timestamp', 'temp_avg', 'temp_diff', 'autocorr', 'max_temp', 'min_temp']
                self.logger.info("Данные успешно извлечены из таблицы weather_data.")
                return pd.DataFrame(records, columns=columns)
        except Exception as e:
            self.logger.error(f"Ошибка при извлечении данных: {e}")
            return pd.DataFrame()

    def delete_data(self, station_id):
        """
        Удаляет данные из таблицы для указанной станции.

        :param station_id: Идентификатор станции.
        """
        query = "DELETE FROM weather_data WHERE station_id = %s"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (station_id,))
                self.connection.commit()
                self.logger.info("Данные успешно удалены из таблицы weather_data.")
        except Exception as e:
            self.logger.error(f"Ошибка при удалении данных: {e}")

    def delete_data_by_period(self, station_id, start_date, end_date):
        """
        Удаляет данные из таблицы для указанной станции за указанный период.

        :param station_id: Идентификатор станции.
        :param start_date: Начальная дата периода (формат YYYY-MM-DD).
        :param end_date: Конечная дата периода (формат YYYY-MM-DD).
        """
        query = "DELETE FROM weather_data WHERE station_id = %s AND DATE(timestamp) BETWEEN %s AND %s"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (station_id, start_date, end_date))
                self.connection.commit()
                self.logger.info(
                    f"Данные для station_id={station_id} за период {start_date} - {end_date} успешно удалены.")
        except Exception as e:
            self.logger.error(
                f"Ошибка при удалении данных для station_id={station_id} за период {start_date} - {end_date}: {e}")

    def close_connection(self):
        """
        Закрывает соединение с базой данных.
        """
        if self.connection:
            self.connection.close()
            self.logger.info("Соединение с базой данных закрыто.")
