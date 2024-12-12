import pandas as pd

import matplotlib.pyplot as plt

from data_analysis.weather_analysis.decorators import time_execution


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
    def __init__(self, data):
        """
        Инициализирует WeatherDataProcessor с заданными параметрами.
        """
        self.df = data



    def calculate_moving_average(self, window: int = 7) -> pd.DataFrame:
        """
        Вычисляет скользящее среднее для средней температуры.

        :param window: Размер окна для скользящего среднего. По умолчанию 7.
        :return:
            pd.DataFrame: DataFrame с добавленной колонкой 'temp_avg'.
        """
        self.df['temp_avg'] = self.df['tavg'].rolling(window=window).mean()
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
        for lag in range(1, len(self.df)):
            autocorr = self.df['tavg'].autocorr(lag=lag)
            self.df['autocorr'] = autocorr
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
    def plot_data(self):
        """
        Строит график средней температуры и скользящего среднего.
        """
        plt.figure(figsize=(12,6))
        plt.plot(self.df['time'], self.df['tavg'], label='Средняя температура')
        plt.plot(self.df['time'], self.df['temp_avg'], label='Скользящее среднее')
        plt.xlabel('Дата')
        plt.ylabel('Температура (°C)')
        plt.title('Анализ временного ряда температуры')
        plt.legend()
        plt.show()

    @time_execution
    def save_to_excel(self, filename: str = 'weather_analysis.xlsx'):
        """
        Сохраняет текущий DataFrame в Excel файл.

        :param filename: Имя файла для сохранения. По умолчанию 'weather_analysis.xlsx'.
        """
        self.df.to_excel(filename, index=False)
