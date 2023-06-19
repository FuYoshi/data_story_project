"""
clean.py
Name: Yoshi Fu
Studentnumber: 13328999
Summary: This script cleans the csv files and combines them.
"""

import numpy as np
import pandas as pd

# PARAMETERS
# Specifiy columns that are not allowed to miss a value.
non_null_columns = ["Country", "Mk", "Cn", "m (kg)", "W (mm)", "Ft"]
fuel_type_filter = ["hydrogen"]
chunk_size = 50000
csv_file_list = [
    "docs/CO2_2015.csv",
    "docs/CO2_2016.csv",
    "docs/CO2_2017.csv",
    "docs/CO2_2018.csv",
    "docs/CO2_2019.csv",
    "docs/CO2_2020.csv",
    "docs/CO2_2021.csv",
]
output_file = "CO2_data.csv"


COLS1 = ["MS", "Mk", "Cn", "m (kg)", "e (g/km)", "w (mm)", "Ft", "Er (g/km)"]
COLS2 = ["MS", "Mk", "Cn", "m (kg)", "Enedc (g/km)", "Ewltp (g/km)", "W (mm)", "Ft", "Ernedc (g/km)", "Erwltp (g/km)"]
COLS3 = ["Country", "Mk", "Cn", "m (kg)", "Enedc (g/km)", "Ewltp (g/km)", "W (mm)", "Ft", "Ernedc (g/km)", "Erwltp (g/km)"]
TYPE1 = {"MS": str, "Mk": str, "Cn": str, "m (kg)": float, "e (g/km)": float, "w (mm)": float, "Ft": str, "Er (g/km)": float}
TYPE2 = {"MS": str, "Mk": str, "Cn": str, "m (kg)": float, "Enedc (g/km)": float, "Ewltp (g/km)": float, "W (mm)": float, "Ft": str, "Ernedc (g/km)": float, "Erwltp (g/km)": float}
TYPE3 = {"Country": str, "Mk": str, "Cn": str, "m (kg)": float, "Enedc (g/km)": float, "Ewltp (g/km)": float, "W (mm)": float, "Ft": str, "Ernedc (g/km)": float, "Erwltp (g/km)": float}
RENAME1 = {"MS": "Country", "e (g/km)": "Enedc (g/km)", "w (mm)": "W (mm)", "Er (g/km)": "Ernedc (g/km)"}
RENAME2 = {"MS": "Country"}


def _read_csv(csv_filename: str):
    """
    Read the csv files. Since the keys changed throughout the years, the
    arguments are set based on the year.
    """
    # Configure the arguments of read_csv.
    if "2015" in csv_filename:
        args = {"usecols": COLS1, "dtype": TYPE1, "encoding": None, "sep": '\t', "rename": RENAME1, "year": 2015}
    elif "2016" in csv_filename:
        args = {"usecols": COLS1, "dtype": TYPE1, "encoding": "utf-16", "sep": '\t', "rename": RENAME1, "year": 2016}
    elif "2017" in csv_filename:
        args = {"usecols": COLS2, "dtype": TYPE2, "encoding": "utf-16", "sep": '\t', "rename": RENAME2, "year": 2017}
    elif "2018" in csv_filename:
        args = {"usecols": COLS2, "dtype": TYPE2, "encoding": None, "sep": '\t', "rename": RENAME2,  "year": 2018}
    elif "2019" in csv_filename:
        args = {"usecols": COLS3, "dtype": TYPE3, "encoding": None, "sep": ',', "rename": None,  "year": 2019}
    elif "2020" in csv_filename:
        args = {"usecols": COLS3, "dtype": TYPE3, "encoding": None, "sep": ',', "rename": None,  "year": 2020}
    elif "2021" in csv_filename:
        args = {"usecols": COLS3, "dtype": TYPE3, "encoding": None, "sep": ',', "rename": None,  "year": 2021}

    # Read the csv using the arguments.
    chunk_container = pd.read_csv(
        csv_filename,
        usecols=args["usecols"],
        dtype=args["dtype"],
        chunksize=chunk_size,
        sep=args["sep"],
        encoding=args["encoding"],
    )

    for chunk in chunk_container:
        # Add missing columns.
        if args["year"] == 2015 or args["year"] == 2016:
            chunk["Ewltp (g/km)"] = np.nan
            chunk["Erwltp (g/km)"] = np.nan
        # Add year.
        chunk["year"] = args["year"]
        if args["rename"]:
            chunk = chunk.rename(columns=args["rename"])
        # Sort columns lexicographically for consistency.
        yield chunk[sorted(chunk.columns)]


def _filter(df: pd.DataFrame):
    """
    Filter out records with missing values.
    Filter out records with hydrogen as fuel type.
    Make fuel type naming convention consistent.
    TODO: make Mk naming convention consistent.
    TODO: make Cn naming convention consistent.
    """
    # Ensure consistent naming convention in fuel types.
    df["Ft"] = df["Ft"].str.lower().str.replace('/', '-').str.replace("unknown", "other").str.strip()

    # Filter out hydrogen and records with missing values.
    df = df[~df["Ft"].isin(fuel_type_filter)]
    df = df[df[non_null_columns].notnull().all('columns')]
    return df


def main():
    """
    Read the csv files and filter them. Append the to the output file.
    """
    append_header = True
    for csv_filename in csv_file_list:
        for chunk in _read_csv(csv_filename):
            chunk = _filter(chunk)

            if append_header:
                chunk.to_csv(output_file, mode="a", index=False)
                append_header = False
            else:
                chunk.to_csv(output_file, mode="a", index=False, header=False)


if __name__ == "__main__":
    main()
