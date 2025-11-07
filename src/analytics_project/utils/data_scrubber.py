# src/analytics_project/utils/data_scrubber.py

import pandas as pd


class DataScrubber:
    """
    Handles data cleaning operations for a DataFrame.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def drop_duplicates(self):
        """Remove duplicate rows."""
        before = len(self.df)
        self.df = self.df.drop_duplicates()
        print(f"Dropped {before - len(self.df)} duplicate rows.")
        return self

    def fill_missing(self, column: str, value):
        """Fill missing values in a column."""
        if column in self.df.columns:
            missing = self.df[column].isna().sum()
            self.df[column] = self.df[column].fillna(value)
            print(f"Filled {missing} missing values in '{column}'.")
        return self

    def clean_column_names(self):
        """Standardize column names (lowercase, underscores)."""
        self.df.columns = self.df.columns.str.strip().str.lower().str.replace(" ", "_")
        print("Cleaned column names.")
        return self

    def get_clean_data(self):
        """Return the cleaned DataFrame."""
        return self.df
