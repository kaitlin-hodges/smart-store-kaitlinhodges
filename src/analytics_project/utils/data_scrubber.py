"""utils/data_scrubber.py.

Reusable utility class for performing common data cleaning and
preparation tasks on a pandas DataFrame.

This class provides methods for:
- Checking data consistency
- Removing duplicates
- Handling missing values
- Filtering outliers
- Renaming and reordering columns
- Formatting strings
- Parsing date fields

Use this class to perform similar cleaning operations across multiple files.
You are not required to use this class, but it shows how we can organize
reusable data cleaning logic - or you can use the logic examples in your own code.

Example:
        from utils.data_scrubber import DataScrubber
        scrubber = DataScrubber(df)
        df = scrubber.remove_duplicate_records().handle_missing_data(fill_value="N/A")

"""

import pandas as pd
from pathlib import Path
from typing import Dict, Tuple, Union, List
import io


class DataScrubber:
    def __init__(self, df: pd.DataFrame):
        """Initialize the DataScrubber with a copy of the DataFrame.
        This ensures the original data is not modified.

        Parameters
        ----------
        df (pd.DataFrame): The DataFrame to be scrubbed.
        """
        self.df = df.copy(deep=True)

    def check_data_consistency_before_cleaning(self) -> dict[str, pd.Series | int]:
        """Check data consistency before cleaning by calculating counts of null and duplicate entries.

        Returns:
        dict: Dictionary with counts of null values and duplicate rows.
        """
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        return {"null_counts": null_counts, "duplicate_count": duplicate_count}

    def check_data_consistency_after_cleaning(self) -> dict[str, pd.Series | int]:
        """Check data consistency after cleaning to ensure there are no null or duplicate entries.

        Returns:
        dict: Dictionary with counts of null values and duplicate rows, expected to be zero for each.
        """
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        assert null_counts.sum() == 0, "Data still contains null values after cleaning."
        assert duplicate_count == 0, "Data still contains duplicate records after cleaning."
        return {"null_counts": null_counts, "duplicate_count": duplicate_count}

    def convert_column_to_new_data_type(self, column: str, new_type: type) -> pd.DataFrame:
        """Convert a specified column to a new data type.

        Parameters
        ----------
        column (str): Name of the column to convert.
        new_type (type): The target data type (e.g., 'int', 'float', 'str').

        Returns
        -------
        pd.DataFrame: Updated DataFrame with the column type converted.

        Raises
        ------
        ValueError: If the specified column not found in the DataFrame.
        """
        try:
            self.df[column] = self.df[column].astype(new_type)
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def drop_columns(self, columns: list[str]) -> pd.DataFrame:
        """Drop specified columns from the DataFrame.

        Parameters
        ----------
                columns (list): List of column names to drop.

        Returns
        -------
                pd.DataFrame: Updated DataFrame with Specified columns removed.

        Raises
        ------
                ValueError: If a specified column is not found in the DataFrame.
        """
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df = self.df.drop(columns=columns)
        return self.df

    def filter_column_outliers(
        self, column: str, lower_bound: Union[float, int], upper_bound: Union[float, int]
    ) -> pd.DataFrame:
        """
        Filter outliers in a specified column based on lower and upper bounds.

        Parameters:
            column (str): Name of the column to filter for outliers.
            lower_bound (float or int): Lower threshold for outlier filtering.
            upper_bound (float or int): Upper threshold for outlier filtering.

        Returns:
            pd.DataFrame: Updated DataFrame with outliers filtered out.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            self.df = self.df[(self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)]
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def format_column_strings_to_lower_and_trim(self, column: str) -> pd.DataFrame:
        """
        Format strings in a specified column by converting to lowercase and trimming whitespace.

        Parameters:
            column (str): Name of the column to format.

        Returns:
            pd.DataFrame: Updated DataFrame with formatted string column.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            self.df[column] = self.df[column].str.lower().str.strip()
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def format_column_strings_to_upper_and_trim(self, column: str) -> pd.DataFrame:
        """
        Format strings in a specified column by converting to uppercase and trimming whitespace.

        Parameters:
            column (str): Name of the column to format.

        Returns:
            pd.DataFrame: Updated DataFrame with formatted string column.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            # Convert to string first to avoid errors on non-string columns, then uppercase and trim
            self.df[column] = self.df[column].astype(str).str.upper().str.strip()
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def clean_data(
        self,
        drop_duplicates: bool = True,
        drop_na: bool = False,
        fill_value: Union[None, float, int, str] = None,
        string_lower: Union[None, List[str]] = None,
        string_upper: Union[None, List[str]] = None,
        date_columns: Union[None, List[str]] = None,
        rename_map: Union[None, Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """
        High-level method to perform a standard cleaning pipeline on the DataFrame.

        Parameters
        ----------
        drop_duplicates: bool
            If True, remove duplicate rows.
        drop_na: bool
            If True, drop rows with any NA values.
        fill_value: optional
            If provided and drop_na is False, fill NA values with this value.
        string_lower: list[str]
            Columns to convert to lowercase and trim.
        string_upper: list[str]
            Columns to convert to uppercase and trim.
        date_columns: list[str]
            Columns to parse as datetime in-place.
        rename_map: dict
            Mapping of old column names to new names.

        Returns
        -------
        pd.DataFrame
            The cleaned DataFrame.
        """
        # Remove duplicates first if requested
        if drop_duplicates:
            try:
                self.remove_duplicate_records()
            except Exception:
                # non-fatal for this orchestration
                pass

        # Handle missing data
        if drop_na:
            self.handle_missing_data(drop=True)
        elif fill_value is not None:
            self.handle_missing_data(fill_value=fill_value)

        # Format string columns to lowercase
        if string_lower:
            for col in string_lower:
                if col in self.df.columns:
                    # ensure string-safe operations
                    self.df[col] = self.df[col].astype(str).str.lower().str.strip()

        # Format string columns to uppercase
        if string_upper:
            for col in string_upper:
                if col in self.df.columns:
                    self.df[col] = self.df[col].astype(str).str.upper().str.strip()

        # Parse date columns in-place
        if date_columns:
            for col in date_columns:
                if col in self.df.columns:
                    self.df[col] = pd.to_datetime(self.df[col], errors="coerce")

        # Rename columns if provided
        if rename_map:
            try:
                self.rename_columns(rename_map)
            except ValueError:
                # If mapping references missing columns, ignore and continue
                pass

        return self.df

    def handle_missing_data(
        self, drop: bool = False, fill_value: Union[None, float, int, str] = None
    ) -> pd.DataFrame:
        """
        Handle missing data in the DataFrame.

        Parameters:
            drop (bool, optional): If True, drop rows with missing values. Default is False.
            fill_value (any, optional): Value to fill in for missing entries if drop is False.

        Returns:
            pd.DataFrame: Updated DataFrame with missing data handled.
        """
        if drop:
            self.df = self.df.dropna()
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
        return self.df

    def inspect_data(self) -> tuple[str, str]:
        """Inspect the data by providing DataFrame information and summary statistics.

        Returns:
            tuple: (info_str, describe_str), where `info_str` is a string representation of DataFrame.info()
                       and `describe_str` is a string representation of DataFrame.describe().
        """
        buffer = io.StringIO()
        self.df.info(buf=buffer)
        info_str = buffer.getvalue()
        describe_str = self.df.describe().to_string()
        return info_str, describe_str

    def parse_dates_to_add_standard_datetime(self, column: str) -> pd.DataFrame:
        """
        Parse a specified column as datetime format and add it as a new column named 'StandardDateTime'.

        Parameters:
            column (str): Name of the column to parse as datetime.

        Returns:
            pd.DataFrame: Updated DataFrame with a new 'StandardDateTime' column containing parsed datetime values.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            self.df['StandardDateTime'] = pd.to_datetime(self.df[column])
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def remove_duplicate_records(self) -> pd.DataFrame:
        """Remove duplicate rows from the DataFrame.

        Returns:
        pd.DataFrame: Updated DataFrame with duplicates removed.
        """
        self.df = self.df.drop_duplicates()
        return self.df

    def rename_columns(self, column_mapping: dict[str, str]) -> pd.DataFrame:
        """Rename columns in the DataFrame based on a provided mapping.

        Parameters
        column_mapping (dict): Dictionary where keys are old column names and values are new names.

        Returns
        pd.DataFrame: Updated DataFrame with renamed columns.

        Raises
        ValueError: If a specified column is not found in the DataFrame.
        """
        for old_name, new_name in column_mapping.items():
            if old_name not in self.df.columns:
                raise ValueError(f"Column '{old_name}' not found in the DataFrame.")

        self.df = self.df.rename(columns=column_mapping)
        return self.df

    def reorder_columns(self, columns: List[str]) -> pd.DataFrame:
        """
        Reorder columns in the DataFrame based on the specified order.

        Parameters:
            columns (list): List of column names in the desired order.

        Returns:
            pd.DataFrame: Updated DataFrame with reordered columns.

        Raises:
            ValueError: If a specified column is not found in the DataFrame.
        """
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df = self.df[columns]
        return self.df


if __name__ == "__main__":
    import pandas as pd
    from pathlib import Path

    # Define paths
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    RAW_DIR = PROJECT_ROOT / "data" / "raw"
    PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

    # Define function to clean customers_data.csv
    def clean_customers_data():
        raw_file = RAW_DIR / "customers_data.csv"
        df = pd.read_csv(raw_file)

        print("RAW CUSTOMERS DATA SAMPLE:\n", df.head())

        # Create scrubber instance
        scrubber = DataScrubber(df)

        # Clean dataset
        cleaned = scrubber.clean_data(
            fill_value="N/A",
            string_upper=["CustomerID", "Name"],
            string_lower=["Region", "PreferredContactMethod"],
            date_columns=["CustomerSince"],
        )
        # Show cleaned preview
        print("\nCLEANED CUSTOMERS DATA SAMPLE:\n", cleaned.head())

        # Save cleaned dataset
        output_file = PROCESSED_DIR / "customers_cleaned.csv"
        cleaned.to_csv(output_file, index=False)
        print(f"\n✅ Cleaned data saved to: {output_file}")

    # Run cleaning function
    clean_customers_data()

    # Define function to clean sales_data.csv
    def clean_sales_data():
        raw_file = RAW_DIR / "sales_data.csv"
        df = pd.read_csv(raw_file)

        print("RAW SALES DATA SAMPLE:\n", df.head())

        # Create scrubber instance
        scrubber = DataScrubber(df)

        # Clean dataset
        cleaned = scrubber.clean_data(
            fill_value="N/A",
            string_lower=["PaymentType"],
            string_upper=[],
            date_columns=["SaleDate"],
        )

        # Show cleaned preview
        print("\nCLEANED DATA SAMPLE:\n", cleaned.head())

        # Save cleaned dataset
        output_file = PROCESSED_DIR / "sales_cleaned.csv"
        cleaned.to_csv(output_file, index=False)
        print(f"\n✅ Cleaned data saved to: {output_file}")

    # Run the function
    clean_sales_data()

    # Clean products_data.csv
    def clean_products_data():
        raw_file = RAW_DIR / "products_data.csv"
        df = pd.read_csv(raw_file)

        print("RAW PRODUCTS DATA SAMPLE:\n", df.head())

        # Create scrubber instance
        scrubber = DataScrubber(df)

        # Clean dataset
        cleaned = scrubber.clean_data(
            fill_value="N/A",
            string_upper=["Condition", "Category"],
            string_lower=["ProductName"],
            date_columns=[],
        )

        # Show cleaned preview
        print("\nCLEANED PRODUCTS DATA SAMPLE:\n", cleaned.head())

        # Save cleaned dataset
        output_file = PROCESSED_DIR / "products_cleaned.csv"
        cleaned.to_csv(output_file, index=False)
        print(f"\n✅ Cleaned data saved to: {output_file}")

    # Run cleaning function
    clean_products_data()
