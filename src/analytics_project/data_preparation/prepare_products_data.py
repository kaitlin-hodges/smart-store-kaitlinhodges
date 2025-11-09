# src/analytics_project/data_preparation/prepare_products_data.py
"""scripts/data_preparation/prepare_products.py

This script reads product data from the data/raw folder, cleans the data, adding columns,
removing duplicates, handling missing values and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

#####################################
# Import Modules
#####################################

# Import from Python Standard Library
import pathlib
import sys

import numpy as np
import pandas as pd

# Ensure project root is in sys.path for local imports
sys.path.append(str(pathlib.Path(__file__).resolve().parents[3]))

from analytics_project.utils.logger import logger

# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
PROJECT_ROOT: pathlib.Path = pathlib.Path(__file__).resolve().parents[3]
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"
REPO_ROOT: pathlib.Path = PROJECT_ROOT.parent

# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Reusable Functions
#####################################


def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"READING: {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if any other error occurs


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV.

    Args:
        df (pd.DataFrame): Cleaned DataFrame.
        file_name (str): Name of the output file.
    """
    logger.info(
        f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}"
    )
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows based on Product ID.

    Keep first occurrence of each Product ID and removes the rest.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    if "ProductID" in df.columns:
        df_deduped = df.drop_duplicates(subset="ProductID").reset_index(drop=True)
    else:
        df_deduped = df.drop_duplicates().reset_index(drop=True)
        logger.warning("No 'ProductID' column found; removed exact duplicates instead.")
    logger.info(f"Original dataframe shape: {df.shape}")
    logger.info(f"Deduped  dataframe shape: {df_deduped.shape}")
    return df_deduped


def add_product_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add columns for StockQuantity and Conditions if missing.
    Added a warning if existing columns are present but contain only missing values.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with new columns added.
    """
    logger.info(f"FUNCTION START: add_product_columns with dataframe shape={df.shape}")

    added_columns = []

    # Add StockQuantity (numeric)
    if "StockQuantity" not in df.columns:
        df["StockQuantity"] = 0.0
        added_columns.append("StockQuantity")
    elif df["StockQuantity"].isna().all():
        logger.warning("Column 'StockQuantity' exists but contains only missing values.")

    # Add Condition
    if "Condition" not in df.columns:
        np.random.seed(42)
        df["Condition"] = np.random.choice(
            ["New", "Used", "Clearance", "Refurbished", "Discontinued"], size=len(df)
        )
        added_columns.append("Condition")
    elif df["Condition"].isna().all():
        logger.warning("Column 'Condition' exists but contains only missing values.")

    if added_columns:
        logger.info(f"Added columns: {', '.join(added_columns)}")
    else:
        logger.info("No new columns added.")

    logger.info(f"Updated dataframe shape: {df.shape}")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values by filling or dropping.
    This logic is specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    missing_before = df.isna().sum().sum()
    logger.info(f"Total missing values before handling: {missing_before}")

    if "StockQuantity" in df.columns:
        df["StockQuantity"].fillna(0, inplace=True)
    if "Condition" in df.columns:
        df["Condition"].fillna("Unknown", inplace=True)

    missing_after = df.isna().sum().sum()
    logger.info(f"Total missing values after handling: {missing_after}")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers based on thresholds.
    This logic is very specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    if "StockQuantity" in df.columns:
        df = df[df["StockQuantity"] >= 0]

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> None:
    """Main function for processing product data."""
    logger.info("==================================")
    logger.info("STARTING prepare_products_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "products_data.csv"
    output_file = "products_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()
    df = add_product_columns(df)

    # Log if any column names changed
    changed_columns = [
        f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    cleaned_shape = df.shape
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {cleaned_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_products_data.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    main()
