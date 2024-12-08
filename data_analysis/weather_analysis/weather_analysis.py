import pandas as pd
from meteostat import Stations, Daily
from datetime import datetime
import matplotlib.pyplot as plt

class WeatherDataProcessor:
    def __init__(self, station_id: str, start: datetime, end: datetime):
        self.station_id = station_id
        self.start = start
        self.end = end
        self.df = self.fetch_data()

    def fetch_data(self) -> pd.DataFrame:
        data = Daily(self.station_id, self.start, self.end)
        data = data.fetch()
        data = data.reset_index()
        return data

    def calculate_moving_average(self, window: int = 7) -> pd.DataFrame:
        self.df['temp_avg'] = self.df['tavg'].rolling(window=window).mean()
        return self.df

    def compute_diff(self) -> pd.DataFrame:
        self.df['temp_diff'] = self.df['tavg'].diff()
        return self.df

    def find_autocorrelation(self, lag: int = 1) -> float:
        autocorr = self.df['tavg'].autocorr(lag=lag)
        return autocorr

    def find_extrema(self) -> pd.DataFrame:
        self.df['max'] = self.df['tavg'][(self.df['tavg'].shift(1) < self.df['tavg']) &
                                         (self.df['tavg'].shift(-1) < self.df['tavg'])]
        self.df['min'] = self.df['tavg'][(self.df['tavg'].shift(1) > self.df['tavg']) &
                                         (self.df['tavg'].shift(-1) > self.df['tavg'])]
        return self.df

    def plot_data(self):
        plt.figure(figsize=(12,6))
        plt.plot(self.df['time'], self.df['tavg'], label='Средняя температура')
        plt.plot(self.df['time'], self.df['temp_avg'], label='Скользящее среднее')
        plt.xlabel('Дата')
        plt.ylabel('Температура (°C)')
        plt.title('Анализ временного ряда температуры')
        plt.legend()
        plt.show()

    def save_to_excel(self, filename: str = 'weather_analysis.xlsx'):
        self.df.to_excel(filename, index=False)
