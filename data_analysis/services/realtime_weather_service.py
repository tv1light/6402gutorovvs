import threading
import time

import pandas as pd

from data_analysis.data_loader.weather_data_loader import WeatherDataLoader
import logging

from data_analysis.weather_analysis.weather_analysis import WeatherDataProcessor


class RealtimeWeatherMonitoringService:
    """
    Сервис для отслеживания изменений в погодных данных в условно реальном времени.
    """

    def __init__(self, service_id, station_id, interval=10, config_path="configs/logging.conf"):
        """
        Инициализация сервиса.

        :param service_id: Идентификатор сервиса.
        :param station_id: Идентификатор станции.
        :param interval: Интервал обновления в секундах.
        :param config_path: Путь к конфигурационному файлу логирования.
        """
        self.service_id = service_id
        self.station_id = station_id
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread = None

        try:
            logging.config.fileConfig(config_path)
        except Exception:
            logging.basicConfig(
                level=logging.WARNING,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[logging.StreamHandler()]
            )
        self.logger = logging.getLogger(f"service_{self.service_id}")

        self.loader = WeatherDataLoader(config_path=config_path)

    def start(self):
        """
        Запускает сервис.
        """
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        self.logger.info("Service %s started.", self.service_id)

    def stop(self):
        """
        Останавливает сервис.
        """
        self.stop_event.set()
        if self.thread:
            self.thread.join()
        self.logger.info("Service %s stopped.", self.service_id)

    def run(self):
        """
        Основной цикл для обновления данных.
        """
        while not self.stop_event.is_set():
            try:
                self.load_and_analyze()
            except Exception as e:
                self.logger.exception("Error in service %s: %s", self.service_id, e)
            time.sleep(self.interval)

    def load_and_analyze(self):
        """
        Загружает данные и выполняет их анализ.
        """
        try:
            data = self.loader.fetch_realtime_data(self.station_id)

            if data is not None and not data.empty:
                processor = WeatherDataProcessor(data)

                # Выполнение анализа
                moving_avg = processor.calculate_moving_average(window=5)
                differential = processor.compute_diff()
                autocorr = processor.find_autocorrelation()
                extrema = processor.find_extrema()

                # Сохранение результатов анализа
                self.save_result_to_file("moving_average", moving_avg)
                self.save_result_to_file("differential", differential)
                self.save_result_to_file("autocorrelation", autocorr)
                self.save_result_to_file("extrema", extrema)

                self.logger.info(f"Service {self.service_id}: Analysis results saved.")
            else:
                self.logger.warning(f"Service {self.service_id}: No data received for station_id={self.station_id}.")
        except Exception as e:
            self.logger.exception(f"Service {self.service_id}: Error during data loading: {e}")

    def save_result_to_file(self, function_name: str, result: pd.DataFrame) -> None:
        """
        Сохраняет результаты анализа в текстовый файл.

        :param function_name: Название функции анализа.
        :param result: DataFrame с результатами анализа.
        """
        try:
            filename = f"analysis_results_{self.service_id}.txt"
            with open(filename, "a") as file:
                file.write(f"\n\nFunction: {function_name}\n")
                file.write(f"Result:\n{result}\n")
                file.write(f"{'-' * 40}\n")
            self.logger.info(f"Service {self.service_id}: Results saved to {filename}.")
        except Exception as e:
            self.logger.exception(f"Service {self.service_id}: Error saving results: {e}")