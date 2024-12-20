import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from data_analysis.data_loader import WeatherDataLoader
import os
os.environ['TESTING'] = 'True'

class TestWeatherDataLoader(unittest.TestCase):
    def setUp(self):
        self.loader = WeatherDataLoader()

    @patch('meteostat.Daily.fetch')
    def test_fetch_historical_data(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame({
            'time': ['2023-01-01', '2023-01-02'],
            'tavg': [5.0, 6.0]
        })
        result = self.loader.fetch_historical_data('12345', '2023-01-01', '2023-01-02')
        self.assertEqual(len(result), 2)
        self.assertIn('tavg', result.columns)

    @patch('meteostat.Daily.fetch')
    def test_fetch_realtime_data(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame({
            'time': ['2023-01-01'],
            'tavg': [5.0]
        })
        result = self.loader.fetch_realtime_data('12345')
        self.assertEqual(len(result), 1)
        self.assertIn('time', result.columns)

if __name__ == '__main__':
    unittest.main()