# -*- coding: utf-8 -*-
"""
Created on Sat Feb 21 19:51:07 2026

@author: Al Fuentes
"""

import numpy as np
import pandas as pd
import re



class TimeSeries:
    
    def __init__(self, df, columns):
        """
        A class to add multiple feature columns (percentage changes, rolling statistics, etc.)
        to a DataFrame while preserving a naming convention that distinguishes columns with suffixes
        from those without (e.g., 'Price', 'Price_idx').
    
        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to modify (in‑place).
        columns : list of str
            List of column names to which calculations will be applied.
        """
        
        self.df = df
        self.columns = columns


    # Apply a function to each column in self.columns and add the result(s) to the DataFrame.
    def apply_to_columns(self, func, **kwargs):
        """
        Parameters
        ----------
        func : callable
            A function with signature func(series, base, suffix, **kwargs) that returns
            either a pandas Series (single new column) or a dict {name: Series} (multiple new columns).
        **kwargs : additional arguments passed to func.
        """
        
        # Split a column name into (base, suffix).
        def splitColName(col):
            """
            Returns
            -------
            (base, suffix) where suffix is None if the name has no underscore suffix.
            """
            match = re.match(r"(\w+)_(\w+)", col)

            if match:
                return match.group(1), match.group(2)   # base, suffix
            else:
                return col, None
        
        
        ColumnsToProcess, DataframeToProcess = self.columns, self.df
        # Add the new columns to the dataframe after the calculations
        for col in ColumnsToProcess:
            base, suffix = splitColName(col)

            result = func(DataframeToProcess[col], base, suffix, **kwargs)

            if isinstance(result, dict):
                for name, series in result.items():
                    DataframeToProcess[name] = series

            else:
                # result is a single Series – we rely on its name being set
                if result.name is None:
                    raise ValueError("Single Series result must have a name")

                DataframeToProcess[result.name] = result


        return DataframeToProcess   # optional, for chaining



    # ----------------------------------------------------------------------
    # Public methods for specific calculations
    # ----------------------------------------------------------------------

    def add_percentage_change(self, periods=5, as_percent=False):
        """
        Add percentage change columns and shifted original columns.

        For each column, two new columns are added:
        - For 'Price'          : 'PriceRise' and '5dayPriceShift'
        - For 'Price_idx'      : 'PriceRise_idx' and '5dayPrice_idxShift'
        """
        def percentageChange(series, base, suffix, periods, as_percent):
            pct = series.pct_change(periods)

            if as_percent:
                pct = pct * 100

            if suffix is None:
                pct_name = f"{periods}day/sRise"
                shift_name = f"{periods}day/sShift"

            else:
                pct_name = f"{periods}day/sRise_{suffix}"
                shift_name = f"{periods}day/sShift_{suffix}"


            return {
                pct_name: pct,
                shift_name: series.shift(periods)}


        return self.apply_to_columns(percentageChange, periods = periods, as_percent = as_percent)



    def add_rolling_mean(self, window=5):
        """
        Add a rolling mean column.

        Naming:
        - 'Price'          -> 'PriceRollingMean_5'
        - 'Price_idx'      -> 'PriceRollingMean_idx_5'
        """

        def rollingAvg(series, base, suffix, window):
            result = series.rolling(window).mean()

            if suffix is None:
                result.name = f"{window}day/sRiseAvg"

            else:
                result.name = f"{window}day/sRiseAvg_{suffix}"

            return result


        return self.apply_to_columns(rollingAvg, window=window)






    def add_rolling_stats(self, window=5, stats=('mean', 'std')):
        """
        Add multiple rolling statistics (mean, std, min, max, etc.) in one pass.

        For each statistic, a new column is created:
        - 'Price'          -> 'PriceRollingMean_5', 'PriceRollingStd_5'
        - 'Price_idx'      -> 'PriceRollingMean_idx_5', 'PriceRollingStd_idx_5'
        """
        
        def _rolling_stats(series, base, suffix, window, stats):
            rolled = series.rolling(window)
            agg = rolled.agg(stats)   # DataFrame with columns named after stats
            new_cols = {}

            for stat in stats:
                if suffix is None:
                    name = f"{base}Rolling{stat.capitalize()}_{window}"

                else:
                    name = f"{base}Rolling{stat.capitalize()}_{suffix}_{window}"

                new_cols[name] = agg[stat]

            return new_cols


        return self.apply_to_columns(_rolling_stats, window=window, stats=stats)

    # Add more methods as needed: add_ewm, add_rolling_median, add_expanding_sum, etc.










# Testing whether everything is working.
if __name__ == "__main__":
    # Sample DataFrame
    data = {
        "Price": [100, 102, 101, 105, 107, 110],
        "Volume": [1000, 1100, 1050, 1200, 1250, 1300],
        "Price_idx": [1.0, 1.02, 1.01, 1.05, 1.07, 1.10],
        "Volume_idx": [1.0, 1.1, 1.05, 1.2, 1.25, 1.3]
    }
    df = pd.DataFrame(data)

    # Create the engineer and add features
    engineer = TimeSeries(df, ["Price", "Volume", "Price_idx", "Volume_idx"])
    engineer.add_percentage_change(periods=3)           # adds 8 columns (2 per original)
    #engineer.add_rolling_mean(window=3)                 # adds 4 columns
    #engineer.add_rolling_stats(window=3, stats=['min', 'max'])  # adds 8 columns

    print(df)
    print(df.columns)
    
    
    # Drop initial NaN rows
    #df = df.dropna()

    #print(df.head())






