"""
clean.py
Name: Yoshi Fu
Studentnumber: 13328999
Summary: This script cleans the csv files and combines them.

Usage: python3 clean.py
Note:
    usage requires that you have the several csv files downloaded from:
        https://www.eea.europa.eu/data-and-maps/data/co2-cars-emission-22
    the csv file has to be renamed to CO2_{year}.csv and should be in the docs
    folder. Note that the site does not give zips for each year, forcing you
    to download 2GB+ of data in some cases. Since the datasets are so big, the
    kernel is likely to crash. Currently, there is NO automatic backup code.
"""

import numpy as np
import pandas as pd

# PARAMETERS
# Specifiy columns that are not allowed to miss a value.
non_null_columns = ["Country", "Mk", "Cn", "m (kg)", "W (mm)", "Ft"]
# Specific fuel types to filter out.
fuel_type_filter = ["hydrogen"]
# All car brands that we care about.
car_makes = [
    "Avtokad", "Aiways", "Alfa Romeo", "Alpina", "Alpine", "Ariel Atom",
    "Aston Martin", "Audi", "BAIC", "Bentley", "Benimar", "BMW", "Brabus",
    "Bugatti", "Buick", "BYD", "Cadillac", "Caterham", "Chevrolet", "Chrysler",
    "Citroën", "CUPRA", "Dacia", "Daewoo", "DFSK", "Daf", "Daihatsu",
    "Dallara", "Datsun", "DeLorean", "Dodge", "Donkervoort", "Dreamer", "DR",
    "DS", "Ferrari", "Fiat", "Fisker", "Ford", "GINAF", "GMC", "Hino Motors",
    "Honda", "Hummer", "Hyundai", "Infiniti", "IVECO", "JAC", "Jaguar", "Jeep",
    "Karma", "KEAT", "Kia", "Koenigsegg", "KTM", "Lada", "Lamborghini",
    "Land Rover", "Lancia", "Lexus", "Lynk & Co", "Lotus", "MAN", "Mahindra",
    "Maserati", "Maxus", "Mazda", "McLaren", "Mercedes", "Mini", "Mitsubishi",
    "Morgan", "Nissan", "Nismo", "Opel", "Pagani", "Peugeot", "Polestar",
    "Porsche", "Puma", "Renault", "Rolls Royce", "Rover", "Range Rover",
    "Saab", "Scania", "Scion", "Seat", "Škoda", "Skywell", "Smart",
    "SsangYong", "Subaru", "Suzuki", "Tesla", "Toyota", "Tripod", "UAZ",
    "Volkswagen", "Volvo", "ZD",
]
# Synonyms that might be used in addition to placeholders.
car_synonyms = {
    "VW": "Volkswagen",
    "Citroen": "Citroën",
    "Skoda": "Skoda",
    "Mazda": "placeholder1",
    "Dreamer": "placeholder2",
}
# Placeholders because if the brand contains DREAMER it also contains DR.
car_placeholders = {
    "placeholder1": "Mazda",
    "placeholder2": "Dreamer",
}
chunk_size = 1000000
csv_file_list = [
    # "docs/CO2_2015.csv",
    # "docs/CO2_2016.csv",
    # "docs/CO2_2017.csv",
    # "docs/CO2_2018.csv",
    # "docs/CO2_2019.csv",
    # "docs/CO2_2020.csv",
    "docs/CO2_2021.csv",
]
output_file = "docs/CO2_data.csv"


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
    Make make naming convention consistent.
    Make commercial name naming convention consistent.
    """
    df = _filter_Ft(df)
    df = _filter_Mk(df)
    df = _filter_Cn(df)
    return df


def _filter_Ft(df: pd.DataFrame):
    """
    Make the naming convention of the fuel type column consistent.
    """
    # Ensure consistent naming convention in fuel types.
    df["Ft"] = df["Ft"].str.lower().str.replace('/', '-').str.replace("unknown", "other").str.strip()

    # Filter out hydrogen and records with missing values.
    df = df[~df["Ft"].isin(fuel_type_filter)]
    df = df[df[non_null_columns].notnull().all('columns')]
    return df


def _filter_Mk(df: pd.DataFrame):
    """
    Make the naming convention of the make column consistent.
    """
    for make in car_makes:
        # Change synonyms that might have been used (i.e. "VW").
        for k, v in car_synonyms.items():
            df.loc[df["Mk"].str.contains(k, case=False), "Mk"] = v

        # Replace str with its brand (i.e. "Mercedes-Benz" -> "Mercedes").
        compare_make = make.strip().replace('-', '').replace(' ', '')
        df.loc[df["Mk"].str.strip().str.replace('-', '').str.replace(' ', '').str.contains(compare_make, case=False), "Mk"] = make

    # Convert placeholders back to their brand. This is because when a brand
    # contains the string "dreamer", it automatically also contains the string
    # "DR". Which is another brand.
    for k, v in car_placeholders.items():
        df.loc[df["Mk"].str.contains(k, case=False), "Mk"] = v

    # Remove everything that is not in the predefined brands.
    df = df[df["Mk"].isin(car_makes)]
    return df


def _filter_Cn(df: pd.DataFrame):
    """
    Convert commercial names to uppercase and strip them.
    """
    df["Cn"] = df["Cn"].str.upper().str.strip()
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
        print(f"Finished csv {csv_filename}")


if __name__ == "__main__":
    main()
