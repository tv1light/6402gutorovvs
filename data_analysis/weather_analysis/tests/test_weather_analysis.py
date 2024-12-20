import unittest
import pandas as pd
from data_analysis.weather_analysis import WeatherDataProcessor
import os
os.environ['TESTING'] = 'True'

class TestWeatherDataProcessor(unittest.TestCase):

    def setUp(self):
        data = pd.DataFrame({
            'time': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-01-06', '2023-01-07',
                     '2023-01-08', '2023-01-09'],
            'tavg': [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, -1., -3, 20.]
        })
        self.processor = WeatherDataProcessor(data)

    def test_calculate_moving_average(self):
        result = self.processor.calculate_moving_average(window=2)
        self.assertIn('temp_avg_2', result.columns)
        self.assertEqual(result['temp_avg_2'].iloc[1], 5.5)

    def test_compute_diff(self):
        result = self.processor.compute_diff()
        self.assertIn('temp_diff', result.columns)
        self.assertEqual(result['temp_diff'].iloc[1], 1.0)

    def test_find_autocorrelation(self):
        result = self.processor.find_autocorrelation()
        self.assertIn('autocorr', result.columns)

    def test_find_extrema(self):
        result = self.processor.find_extrema()
        self.assertIn('max', result.columns)
        self.assertIn('min', result.columns)

if __name__ == '__main__':

    unittest.main()


