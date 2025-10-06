import re
import pandas as pd
import logging
from data_ingestion import read_from_web_CSV  # Make sure this is correctly imported

class WeatherDataProcessor:
    def __init__(self, config_params, logging_level="INFO"):
        self.weather_station_data_url = config_params['weather_csv_path']
        self.weather_mapping_url = config_params.get('weather_mapping_csv', None)
        self.patterns = config_params['regex_patterns']
        self.weather_df = pd.DataFrame()  # Initialize as empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        # Load weather CSV
        self.weather_df = read_from_web_CSV(self.weather_station_data_url)
        self.logger.info(f"Weather data loaded ({len(self.weather_df)} rows).")

        # Drop index column if present
        if 'Unnamed: 0' in self.weather_df.columns:
            self.weather_df = self.weather_df.drop(columns=['Unnamed: 0'])

        # Merge with mapping CSV if available
        if self.weather_mapping_url:
            mapping_df = read_from_web_CSV(self.weather_mapping_url)
            if 'Unnamed: 0' in mapping_df.columns:
                mapping_df = mapping_df.drop(columns=['Unnamed: 0'])
            # Merge using weather station ID
            self.weather_df = self.weather_df.merge(
                mapping_df, left_on='Weather_station_ID', right_on='Weather_station', how='left'
            )
            self.logger.info("Weather station mapping merged.")


    def extract_measurement(self, message):
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        if not self.weather_df.empty:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is empty, skipping message processing.")

    def calculate_means(self):
        if 'Measurement' not in self.weather_df.columns or 'Value' not in self.weather_df.columns:
            self.logger.warning("Cannot calculate means: 'Measurement' or 'Value' columns missing.")
            return pd.DataFrame()
        means = self.weather_df.groupby(['Weather_station_ID', 'Measurement'])['Value'].mean()
        self.logger.info("Mean values calculated.")
        return means.unstack()


    def process(self):
        """Run the full pipeline: load data, extract measurements, calculate means."""
        self.weather_station_mapping()
        self.process_messages()
        self.logger.info("Weather data processing completed.")
        return self.weather_df
