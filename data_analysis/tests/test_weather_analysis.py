import unittest
import numpy as np
import pandas as pd

from data_analysis.weather_analysis.weather_analysis import WeatherDataProcessor


class TestWeatherDataProcessor(unittest.TestCase):
    """
    Класс для тестирования функциональности WeatherDataProcessor.
    """

    @classmethod
    def setUpClass(cls):
        """
        Метод, выполняющийся перед запуском всех тестов.
        Инициализирует экземпляр WeatherDataProcessor с заданными параметрами.
        """
        data = pd.DataFrame({
            'time': pd.date_range(start="2023-01-01", periods=10, freq='D'),
            'tavg': np.arange(10)  # Simple increasing values
        })
        cls.processor = WeatherDataProcessor(data)

    def test_calculate_moving_average(self):
        """
        Тестирует вычисление скользящего среднего.
        Проверяет наличие колонки 'temp_avg' в DataFrame.
        """
        df = self.processor.calculate_moving_average(window=5)
        self.assertIn('temp_avg_5', df.columns)

    def test_compute_diff(self):
        """
        Тестирует вычисление дифференциала средней температуры.
        Проверяет наличие колонки 'temp_diff' в DataFrame.
        """
        df = self.processor.compute_diff()
        self.assertIn('temp_diff', df.columns)

    def test_find_autocorrelation(self):
        """
        Тестирует вычисление автокорреляции.
        Проверяет, что результат является числом типа float.
        """
        autocorr = self.processor.find_autocorrelation()
        self.assertIn('autocorr', autocorr.columns)

    def test_find_extrema(self):
        """
        Тестирует поиск экстремумов.
        Проверяет наличие колонок 'max' и 'min' в DataFrame.
        """
        df = self.processor.find_extrema()
        self.assertIn('max', df.columns)
        self.assertIn('min', df.columns)

if __name__ == '__main__':
    unittest.main()
