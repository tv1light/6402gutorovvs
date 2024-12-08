import unittest
from datetime import datetime
from data_analysis.weather_analysis.weather_analysis import WeatherDataProcessor


class TestWeatherDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Используем станцию Самары (код 28900)
        cls.processor = WeatherDataProcessor(
            station_id='28900',
            start=datetime(2024, 1, 1),
            end=datetime(2024, 12, 8)
        )

    def test_fetch_data(self):
        self.assertFalse(self.processor.df.empty, "Данные не были загружены")

    def test_calculate_moving_average(self):
        df = self.processor.calculate_moving_average(window=3)
        self.assertIn('temp_avg', df.columns, "Отсутствует колонка temp_avg")

    def test_compute_diff(self):
        df = self.processor.compute_diff()
        self.assertIn('temp_diff', df.columns, "Отсутствует колонка temp_diff")

    def test_find_autocorrelation(self):
        autocorr = self.processor.find_autocorrelation(lag=1)
        self.assertIsInstance(autocorr, float, "Autocorrelation должно быть числом")

    def test_find_extrema(self):
        df = self.processor.find_extrema()
        self.assertIn('max', df.columns, "Отсутствует колонка max")
        self.assertIn('min', df.columns, "Отсутствует колонка min")

if __name__ == '__main__':
    unittest.main()
