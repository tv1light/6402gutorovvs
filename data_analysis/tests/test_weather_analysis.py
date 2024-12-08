import unittest
from datetime import datetime
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
        cls.processor = WeatherDataProcessor(
            # Используем станцию Самары (код 28900)
            station_id='28900',
            start=datetime(2024, 1, 1),
            end=datetime(2024, 12, 8)
        )

    def test_fetch_data(self):
        """
        Тестирует загрузку данных.
        Проверяет, что DataFrame не пустой.
        """
        self.assertFalse(self.processor.df.empty, "Данные не были загружены")

    def test_calculate_moving_average(self):
        """
        Тестирует вычисление скользящего среднего.
        Проверяет наличие колонки 'temp_avg' в DataFrame.
        """
        df = self.processor.calculate_moving_average(window=3)
        self.assertIn('temp_avg', df.columns, "Отсутствует колонка temp_avg")

    def test_compute_diff(self):
        """
        Тестирует вычисление дифференциала средней температуры.
        Проверяет наличие колонки 'temp_diff' в DataFrame.
        """
        df = self.processor.compute_diff()
        self.assertIn('temp_diff', df.columns, "Отсутствует колонка temp_diff")

    def test_find_autocorrelation(self):
        """
        Тестирует вычисление автокорреляции.
        Проверяет, что результат является числом типа float.
        """
        autocorr = self.processor.find_autocorrelation(lag=1)
        self.assertIsInstance(autocorr, float, "Autocorrelation должно быть числом")

    def test_find_extrema(self):
        """
        Тестирует поиск экстремумов.
        Проверяет наличие колонок 'max' и 'min' в DataFrame.
        """
        df = self.processor.find_extrema()
        self.assertIn('max', df.columns, "Отсутствует колонка max")
        self.assertIn('min', df.columns, "Отсутствует колонка min")

if __name__ == '__main__':
    unittest.main()
