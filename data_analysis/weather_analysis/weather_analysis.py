import pandas as pd

from data_analysis.weather_analysis.decorators import time_execution
from data_analysis.weather_analysis.generators import generate_autocorr
from data_analysis.database_manager.database_manager import WeatherDatabaseManager

class WeatherDataProcessor:
    """
    Класс для обработки и анализа погодных данных.

    Атрибуты:
        station_id (str): Идентификатор станции погоды.
        start (datetime): Начальная дата периода анализа.
        end (datetime): Конечная дата периода анализа.
        df (pd.DataFrame): DataFrame с загруженными данными.
    """
    @time_execution
    def __init__(self, data, db_manager=None):
        """
        Инициализирует WeatherDataProcessor с заданными параметрами.
        """
        self.df = data
        self.db_manager = db_manager

    def calculate_moving_average(self, window: int = 7) -> pd.DataFrame:
        """
        Вычисляет скользящее среднее для средней температуры.

        :param window: Размер окна для скользящего среднего. По умолчанию 7.
        :return:
            pd.DataFrame: DataFrame с добавленной колонкой 'temp_avg'.
        """
        self.df[f'temp_avg_{window}'] = self.df['tavg'].rolling(window=window).mean()
        return self.df

    @time_execution
    def compute_diff(self) -> pd.DataFrame:
        """
        Вычисляет разницу (дифференциал) средней температуры.

        :return:
            pd.DataFrame: DataFrame с добавленной колонкой 'temp_diff'.
        """
        self.df['temp_diff'] = self.df['tavg'].diff()
        return self.df

    @time_execution
    def find_autocorrelation(self) -> pd.DataFrame:
        """
        Вычисляет автокорреляцию средней температуры с заданным лагом.

        :param lag: Лаг для автокорреляции. По умолчанию 1.
        :return:
            autocorr: Значение автокорреляции.
        """

        self.df['autocorr'] = list(generate_autocorr(self.df, column='tavg'))
        return self.df

    @time_execution
    def find_extrema(self) -> pd.DataFrame:
        """
        Находит максимумы и минимумы средней температуры.

        :return:
            pd.DataFrame: DataFrame с добавленными колонками 'max' и 'min'.
        """
        self.df['max'] = self.df['tavg'][(self.df['tavg'].shift(1) < self.df['tavg']) &
                                         (self.df['tavg'].shift(-1) < self.df['tavg'])]
        self.df['min'] = self.df['tavg'][(self.df['tavg'].shift(1) > self.df['tavg']) &
                                         (self.df['tavg'].shift(-1) > self.df['tavg'])]
        return self.df

    @time_execution
    def save_to_database(self, station_id: str):
        """
        Сохраняет результаты анализа в базу данных.

        :param station_id: Идентификатор станции.
        """
        if not self.db_manager:
            raise ValueError("Database manager is not configured.")

        records = []
        for index, row in self.df.iterrows():
            records.append({
                'station_id': station_id,
                'timestamp': row['time'],
                'temp_avg': row.get(f'temp_avg_7'),
                'temp_diff': row.get('temp_diff'),
                'autocorr': row.get('autocorr'),
                'max_temp': row.get('max'),
                'min_temp': row.get('min')
            })

        self.db_manager.insert_data(records)

    @time_execution
    def save_to_excel(self, filename: str = 'weather_analysis.xlsx'):
        """
        Сохраняет текущий DataFrame в Excel файл.

        :param filename: Имя файла для сохранения. По умолчанию 'weather_analysis.xlsx'.
        """
        self.df.to_excel(filename, index=False)

    def close_database(self):
        """
        Закрывает соединение с базой данных, если оно открыто.
        """
        if self.db_manager:
            self.db_manager.close_connection()

    def calculate_all_params(self):
        self.df = self.calculate_moving_average()
        self.df = self.compute_diff()
        self.df = self.find_autocorrelation()
        self.df = self.find_extrema()
        return self.df
