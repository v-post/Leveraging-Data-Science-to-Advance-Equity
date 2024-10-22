"""
File: derog_finder.py
Author: Veronika Post
Date: May 5 2024
Description: This script is a part of the Leveraging Data Science to Advance Equity project. 
Currently the script works on Unix/Linux systems.
Usage: The script requires Python >= 3.7 as it guarantees odered dictionaries data structure.
       It's recommended to install all the dependancies from the requirements.txt 
       within a virtual environment. To get the usage info run: derog_finder.py. Usage:
       python derog_finder.py data=<zip_file_path> term=<term> tag=<tag> map=<yes/no/all> output=<csv/xlsx>.
       The order of the provided arguments matters.
"""

# Necessary library imports
import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import zipfile
import folium
from datetime import datetime
import warnings

# Declare global variables
DATA = None
TERM = None
TAG = None
MAP = None
OUTPUT = None
MAIN_DF = None


def read_data(DATA):
    global MAIN_DF
    # Read the data in the dataframe
    # Extract occurrence.txt from the zip archive
    with zipfile.ZipFile(DATA, 'r') as zip_ref:
        zip_ref.extract('occurrence.txt')
        # read the extracted occurrence.txt into a DataFrame
    try:
        MAIN_DF = pd.read_csv('occurrence.txt', sep='\t', on_bad_lines='skip')
    except Exception as e:
        print(f"Error reading occurrence.txt: {e}")


# Find the provided derogatory term in the provided datafarme (lower case)
def search_for_word(word, df_lower):
    
    # search for the word in both columns; return the row index if word is found
    def search_columns(row):
        if word in row['locality'] or word in row['occurrenceRemarks']:
            return True
        return False

    # get a list of indexes; will be empty if nothing found
    found_indexes = df_lower[df_lower.apply(search_columns, axis=1)].index.tolist()

    print('Found ' + str(len(found_indexes)) + ' rows with the word "' + word +'".')
    return found_indexes


# The function returns the terms that do not have a specific tag in the locality or occurrenceRemarks columns
# Return the list of the row indexes and the dataframe based on those values
def search_by_word_and_tag(word, tag, df_lower, df):

    # for the found raws if the locality column has "historical term" ("historical name"?)
    # search for the word in both columns; return the row index if word is found
    def search_word(row):
        if word in row['locality'] or word in row['occurrenceRemarks']:
            return True
        return False

    # get a list of indexes; will be empty if nothing found
    indexes = df_lower[df_lower.apply(search_word, axis=1)].index.tolist()

    # subset the dataframe on the found indexes
    found_df = df.loc[indexes]

    # from the data subset exclude the rows that have been tagged
    def search_tag(row):
        # check for NaN values in occurenceRemarks:
        if isinstance(row['locality'], str) and tag in row['locality']:
            return False
        if isinstance(row['occurrenceRemarks'], str) and tag in row['occurrenceRemarks']:
            return False
        return True
    
    # get a list of indexes; will be empty if nothing found
    not_identified_indexes = found_df[found_df.apply(search_tag, axis=1)].index.tolist()
    print('Found ' + str(len(not_identified_indexes)) + ' untagged rows with the word "' + word +'".')
    not_identified_df = df.loc[not_identified_indexes]
    return [not_identified_indexes, not_identified_df]


# Plot the data
def plot_all_terms(terms_df, filename_base):
    # Make sure that there are no empty values in the latitude and longitude columns
    coords_df = terms_df.dropna(subset=['decimalLatitude', 'decimalLongitude'])

    # Map of all the terms "Indian" with the coordinates
    terms_map = folium.Map(location=[30, 10], tiles="cartodb positron", zoom_start=2)

    # plot all the occurences
    if filename_base == "all_records":
        coords_df.apply(lambda row: folium.CircleMarker(location=[row['decimalLatitude'], row['decimalLongitude']],
                                                        radius=1.5, fill_opacity=0.7, 
                                                        popup=row['references']).add_to(terms_map), axis=1)
    else:
        coords_df.apply(lambda row: folium.Marker(location=[row['decimalLatitude'], row['decimalLongitude']],
                                             popup=row['references']).add_to(terms_map), axis=1)
    # save it as an html object which is interactive in a browser
    map_name = f"./outputs/{filename_base}.html"
    terms_map.save(map_name)
    print(f"Map {filename_base}.html has been saved in the directory 'outputs'.")


def main(**kwargs):

    global DATA
    global TERM
    global TAG
    global MAP
    global OUTPUT
    global MAIN_DF

    # Define constants, package them in a list
    CONSTS_LIST = [ [DATA], [TERM], [TAG], [MAP], [OUTPUT]] 

    # Save the provided arguments into the script constants
    for argument, new_value in zip(CONSTS_LIST, kwargs.values()):
        argument[0] = new_value

    # Update variables using a loop
    DATA, TERM, TAG, MAP, OUTPUT = (arg[0] for arg in CONSTS_LIST)
    print(DATA, TERM, TAG, MAP, OUTPUT)

    # Silence Pandas warnings
    warnings.filterwarnings("ignore")
    print("Reading data...")
    
    # Read the data in
    read_data(DATA)

    # Converting the type of the year, month, and day columns into ints, imputing zeroes instead of Nan-s.
    MAIN_DF['year'] = MAIN_DF['year'].fillna(0).astype(int)

    # Create a subset of the database
    occurRem_loc_df = MAIN_DF[['id', 'catalogNumber', 'occurrenceRemarks', 'locality', 
                                        'decimalLatitude', 'decimalLongitude', 'year', 'references']]

    # Create a copy of the dataframe subset
    occurRem_loc_df_lower  = occurRem_loc_df.copy(deep=True)

    # Convert two columns to lower case
    occurRem_loc_df_lower['locality'] = occurRem_loc_df['locality'].astype(str).str.lower()
    occurRem_loc_df_lower['occurrenceRemarks'] = occurRem_loc_df['occurrenceRemarks'].astype(str).str.lower()

    # Find the derogatory terms
    all_found_terms_indexes = search_for_word(TERM, occurRem_loc_df_lower)

    # Now we are searching for these words, but do account for "historical term" in the locality column
    terms_untagged, terms_untagged_df = search_by_word_and_tag(TERM, TAG, occurRem_loc_df_lower, occurRem_loc_df)


    # Output
    # Write the file in the requested format into the "outputs" directory - create it if needed
    output_dir = 'outputs'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # The output file name is "term" + "tag" + current date and time
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename_base = f"{TERM}_{TAG}_{timestamp}"


    # Save as csv
    if OUTPUT == "csv":
        filename = f"{filename_base}.csv"
        file_path = os.path.join(output_dir, filename)
        terms_untagged_df.to_csv(file_path, index=False) 
        print(f"File {filename} has been created in the directory '{file_path}'.")
    
    # Save as Excel
    if OUTPUT == "xlsx":
        filename = f"{filename_base}.xlsx"
        file_path = os.path.join(output_dir, filename)
        terms_untagged_df.to_excel(file_path, index=False) 
        print(f"File {filename} has been created in the directory '{file_path}'.")


    # If plotting requested - plot
    if MAP == "yes":
        plot_all_terms(occurRem_loc_df.loc[all_found_terms_indexes], filename_base)

    # If plotting of all possible values expected requested - plot
    if MAP == "all":
        plot_all_terms(occurRem_loc_df, "all_records")


# To run it as a standalone cli script
if __name__ == "__main__":

    # If no arguments or too many - provide print usage and exit
    if (len(sys.argv) < 2) or (len(sys.argv) > 6):
        print("Usage: python derog_finder.py data=<zip_file_path> term=<term> tag=<tag> map=<all/yes/no> output=<csv/xlsx>")
        sys.exit(1)
    # Parse arguments, separate keys and values, write them into a dictionary 
    kwargs = {}
    for arg in sys.argv[1:]:
        if '=' in arg:
            key, value = arg.split('=', 1)
            kwargs[key] = value

    # Accept any number of key value pairs as arguments
    main(**kwargs)
