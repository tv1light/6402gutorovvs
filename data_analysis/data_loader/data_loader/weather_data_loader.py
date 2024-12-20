from meteostat import Daily
import datetime
import pandas as pd
import logging
import logging.config
from datetime import timedelta

class WeatherDataLoader:
    """
    Класс для получения погодных данных с использованием station_id.
    """

    def __init__(self, config_path="configs/logging.conf"):
        """
        Инициализация загрузчика данных.

        :param config_path: Путь к конфигурации логирования.
        """
        try:
            logging.config.fileConfig(config_path)
        except Exception:
            logging.basicConfig(
                level=logging.WARNING,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[logging.StreamHandler()]
            )
        self.logger = logging.getLogger("loader")

    def fetch_historical_data(self, station_id: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Загружает исторические погодные данные на основе station_id.

        :param station_id: Идентификатор станции.
        :param start_date: Начальная дата.
        :param end_date: Конечная дата.
        :return: DataFrame с историческими данными.
        """
        try:
            data = Daily(station_id, start_date, end_date)
            df = data.fetch()

            if df.empty:
                self.logger.warning("No data available for station: %s for the period %s - %s.", station_id, start_date, end_date)
                return pd.DataFrame()

            self.logger.info("Historical data successfully loaded for station %s.", station_id)
            return df.reset_index()
        except Exception as e:
            self.logger.exception("Error while loading historical data: %s", e)
            return pd.DataFrame()

    def fetch_realtime_data(self, station_id: str) -> pd.DataFrame:
        """
        Загружает данные условного реального времени на основе station_id.

        :param station_id: Идентификатор станции.
        :return: DataFrame с данными за текущий день или за предыдущий день, если данные за сегодня отсутствуют.
        """
        try:
            today = datetime.datetime.now()
            data = Daily(station_id, today, today)
            df = data.fetch()
            df.reset_index()
            if df.empty:
                self.logger.warning("No data available for today for station: %s. Trying previous day.", station_id)
                yesterday = today - timedelta(days=10)
                data = Daily(station_id, yesterday, today)
                df = data.fetch()
                df.reset_index()
                if df.empty:
                    self.logger.warning("No data available for previous day for station: %s.", station_id)
                    return pd.DataFrame()

                self.logger.info("Data successfully loaded for the previous day for station %s.", station_id)
                return df.reset_index()

            self.logger.info("Realtime data successfully loaded for station %s.", station_id)
            return df.reset_index()
        except Exception as e:
            self.logger.exception("Error while loading realtime data: %s", e)
            return pd.DataFrame()
