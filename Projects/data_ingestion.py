"""
data_ingestion.py
-----------------
This module provides functions for ingesting data from SQLite databases and CSV files
(local or remote) for the Maji Ndogo agricultural survey project.

It includes:
- Database connection handling
- Querying database tables
- Loading CSV data
- Merging multiple DataFrames
- Closing database connections
- Logging support for tracking data ingestion

Author: Your Name
"""

from functools import reduce
import sqlite3
import pandas as pd
import os
import logging
from sqlalchemy import create_engine, text

# =========================
# LOGGER SETUP
# =========================
logger = logging.getLogger('data_ingestion')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# =========================
# DATABASE HANDLING
# =========================

def find_database_file():
    """
    Searches for the first SQLite database (.db) file in the current directory.

    Returns
    -------
    str or None
        The filename of the first .db file found, or None if no database is found.
    """
    for file in os.listdir('.'):
        if file.endswith('.db'):
            print(f"Database file found: {file}")
            return file
    print("No database file found in the current directory.")
    return None


def connect_to_db(db_path=None):
    """
    Connects to a SQLite database.

    Parameters
    ----------
    db_path : str, optional
        Path to the SQLite database file. If None, attempts to find one in the current directory.

    Returns
    -------
    sqlite3.Connection
        Connection object to the SQLite database.

    Raises
    ------
    FileNotFoundError
        If no database file is found.
    """
    if db_path is None:
        db_path = find_database_file()
        if db_path is None:
            raise FileNotFoundError("No database file found in the current directory.")
    
    try:
        conn = sqlite3.connect(db_path)
        print(f"Connected to database: {db_path}")
        return conn
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None


def fetch_table_data(conn, table_name):
    """
    Fetches all rows from a specified table in the SQLite database.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.
    table_name : str
        Name of the table to fetch.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing all rows from the table. Returns empty DataFrame if an error occurs.
    """
    try:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)
        print(f"Loaded table '{table_name}' successfully ({len(df)} records).")
        return df
    except Exception as e:
        print(f"Error fetching table '{table_name}': {e}")
        return pd.DataFrame()


# =========================
# CSV HANDLING
# =========================

def load_csv_data(filepath_or_url):
    """
    Loads CSV data from a local path or a web URL.

    Parameters
    ----------
    filepath_or_url : str
        Local file path or HTTP/HTTPS URL to a CSV file.

    Returns
    -------
    pandas.DataFrame
        Loaded DataFrame. Returns empty DataFrame if an error occurs.
    """
    try:
        df = pd.read_csv(filepath_or_url)
        source_type = "remote" if filepath_or_url.startswith(("http://", "https://")) else "local"
        print(f"Loaded {source_type} CSV: {filepath_or_url} ({len(df)} rows, {len(df.columns)} columns)")
        return df
    except Exception as e:
        print(f"Error reading CSV data from {filepath_or_url}: {e}")
        return pd.DataFrame()


def read_from_web_CSV(URL):
    """
    Reads a CSV file directly from a web URL.

    Parameters
    ----------
    URL : str
        HTTP/HTTPS URL pointing to a CSV file.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the CSV data.

    Raises
    ------
    Exception
        If reading the CSV fails.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e


# =========================
# MERGE MULTIPLE DATAFRAMES
# =========================

def merge_multiple_dataframes(dfs, on, how="inner"):
    """
    Merges a list of pandas DataFrames on a specified column.

    Parameters
    ----------
    dfs : list of pandas.DataFrame
        List of DataFrames to merge.
    on : str
        Column name to merge on.
    how : str, optional
        Merge method ('inner', 'outer', 'left', 'right'). Default is 'inner'.

    Returns
    -------
    pandas.DataFrame
        Merged DataFrame.

    Raises
    ------
    ValueError
        If the input list is empty.
    """
    if not dfs:
        raise ValueError("No dataframes provided for merging.")
    
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=on, how=how), dfs)
    print(f"Successfully merged {len(dfs)} DataFrames on '{on}' → {len(merged_df)} rows.")
    return merged_df


# =========================
# DATABASE ENGINE AND QUERY
# =========================

def create_db_engine(db_path):
    """
    Creates a SQLAlchemy engine for connecting to the database.

    Parameters
    ----------
    db_path : str
        Path or URI to the database.

    Returns
    -------
    sqlalchemy.engine.Engine
        SQLAlchemy engine connected to the database.

    Raises
    ------
    ImportError
        If SQLAlchemy is not installed.
    Exception
        If engine creation fails.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        logger.info("Database engine created successfully.")
        return engine
    except ImportError as e:
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e


def query_data(engine, sql_query):
    """
    Executes a SQL query using the provided SQLAlchemy engine.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Active SQLAlchemy database engine.
    sql_query : str
        SQL query string to execute.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing query results.

    Raises
    ------
    ValueError
        If the query returns no rows.
    Exception
        For other errors during query execution.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e:
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e


# =========================
# CLOSE DATABASE CONNECTION
# =========================

def close_connection(conn):
    """
    Safely closes a SQLite database connection.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection to close.
    """
    if conn:
        conn.close()
        print("Database connection closed.")
