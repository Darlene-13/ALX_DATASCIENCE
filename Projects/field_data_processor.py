import pandas as pd
import logging
from data_ingestion import create_db_engine, query_data, read_from_web_CSV


class FieldDataProcessor:
    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes the FieldDataProcessor class with a configuration dictionary.
        """
        # --- Configuration from the dictionary ---
        self.db_path = config_params.get("db_path", "sqlite:///Maji_Ndogo_farm_survey_small.db")
        self.sql_query = config_params.get("sql_query", "")
        self.columns_to_rename = config_params.get("columns_to_rename", {})
        self.values_to_rename = config_params.get("values_to_rename", {})
        self.weather_mapping_URL = config_params.get(
            "weather_mapping_csv",
            "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv",
        )

        # --- Initialize logging ---
        self.initialize_logging(logging_level)

        # --- Placeholders ---
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """Sets up instance-specific logging."""
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        # Determine log level
        level_map = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "NONE": None}
        if isinstance(logging_level, str):
            level = level_map.get(logging_level.upper(), logging.INFO)
        else:
            level = logging.INFO

        if level is None:
            self.logger.disabled = True
            return

        self.logger.setLevel(level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    # ------------------------------
    # DATA INGESTION
    # ------------------------------
    def ingest_sql_data(self):
        """Connects to the database, executes the SQL query, and stores the result in self.df."""
        try:
            self.engine = create_db_engine(self.db_path)
            self.df = query_data(self.engine, self.sql_query)
            self.logger.info("Successfully loaded data from the database.")
            return self.df
        except Exception as e:
            self.logger.error(f"Error loading data from database: {e}")
            raise

    # ------------------------------
    # COLUMN RENAMING
    # ------------------------------
    def rename_columns(self):
        """Swaps the names of two columns in self.df according to self.columns_to_rename."""
        if self.df is None or self.df.empty:
            self.logger.warning("No data loaded. Cannot rename columns.")
            return

        if not hasattr(self, "columns_to_rename") or not self.columns_to_rename:
            self.logger.warning("No columns specified for renaming.")
            return

        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        self.logger.info(f"Swapped columns: {column1} with {column2}")

    # ------------------------------
    # VALUE CORRECTIONS
    # ------------------------------
    def apply_corrections(self, column_name="Crop_type", abs_column="Elevation"):
        """
        Applies corrections to the DataFrame:
        1. Takes the absolute value of a numeric column.
        2. Renames values in a categorical column using self.values_to_rename mapping.
        """
        if self.df is None or self.df.empty:
            self.logger.warning("No data loaded. Cannot apply corrections.")
            return

        # Absolute numeric correction
        if abs_column in self.df.columns:
            self.df[abs_column] = self.df[abs_column].abs()
            self.logger.debug(f"Applied absolute value correction to '{abs_column}'.")

        # Crop renaming correction
        if column_name in self.df.columns:
            self.df[column_name] = self.df[column_name].apply(
                lambda crop: self.values_to_rename.get(crop, crop)
            )
            self.logger.debug(f"Applied crop value corrections in '{column_name}'.")

    # ------------------------------
    # WEATHER MAPPING
    # ------------------------------
    def weather_station_mapping(self):
        """Reads and merges weather station mapping CSV with the main DataFrame."""
        mapping_df = read_from_web_CSV(self.weather_mapping_URL)

        if "Unnamed: 0" in mapping_df.columns:
            mapping_df = mapping_df.drop(columns=["Unnamed: 0"])

        if self.df is not None and not self.df.empty:
            self.df = self.df.merge(mapping_df, on="Field_ID", how="left")
            self.logger.info(f"Merged weather station mapping. Columns now: {len(self.df.columns)}")
        else:
            self.logger.warning("Main DataFrame empty. Returning mapping only.")
            return mapping_df

    # ------------------------------
    # FULL PROCESS PIPELINE
    # ------------------------------
    def process(self):
        """
        Executes the full data processing pipeline:
        1. Load SQL data
        2. Rename columns
        3. Apply value corrections
        4. Merge weather station mapping
        5. Return the processed DataFrame
        """
        self.logger.info("Starting full data processing pipeline...")

        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()

        self.logger.info("Data processing complete.")
        return self.df
