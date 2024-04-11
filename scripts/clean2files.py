import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
import time


spark = SparkSession.builder.appName("demo").config("spark.driver.memory", "8g").config("spark.executor.memory", "8g").getOrCreate()

#####File Locations
file_locations = [
    "./data/combined_world_development.csv",
    "./data/WHO_statistics/eliminateViolenceAgainstWomen.csv"
]

## function to clean countries which are not in the European Union
def filter_eu_countries(chunk, column_name):
    lst_European_Union_countries = [ "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia", "Denmark", "Estonia",
    "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg",
    "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"]
    return chunk[chunk[column_name].isin(lst_European_Union_countries)]

# clean non-European countries
def clean_and_leave_EU_coutries():
    for i, file_location in enumerate(file_locations):
        filtered_chunks = []
        if i == 0 or i==3:
            column_name = "Country"
        elif i ==1 or i==2:
            column_name = "Country Name"
        elif i >= 4:
            column_name = "Location"

        for chunk in pd.read_csv(file_location, chunksize=50, encoding='latin1'):
            filtered_chunk = filter_eu_countries(chunk, column_name)
            filtered_chunks.append(filtered_chunk)

        # Concatenate filtered chunks and save the result
        filtered_df = pd.concat(filtered_chunks)
        filtered_df.to_csv(f"./clean/filtered_file_{i+1}.csv", index=False) # creation of files 


def process_chunk_by_year(chunk, first_year, last_year, cols, newColName):
    years_columns = [str(year) for year in range(first_year, last_year + 1)]
    cols_to_melt = [col for col in chunk.columns if any(year in col.split(' ')[0] for year in years_columns)]
    if not cols_to_melt:
        print("No year columns found in the chunk.")
        return None
    
    id_vars = [col for col in chunk.columns if col not in cols_to_melt]
    melted_chunk = pd.melt(chunk, id_vars=id_vars, value_vars=cols_to_melt, var_name="Period", value_name=newColName)
    return melted_chunk


def min_max_years(chunk):
    years_columns, other_columns = [], []
    for col in chunk.columns:
        if any(char.isdigit() for char in col) and 'Unnamed' not in col:
            clean_col = col.split(' ')[0]
            years_columns.append(clean_col)
        else:
            other_columns.append(col)

    #renamed_chunk = chunk.rename(columns={col: col.split(' ')[0] for col in years_columns})

    min_year_col = min(years_columns) if years_columns else None
    max_year_col = max(years_columns) if years_columns else None

    return [min_year_col, max_year_col, other_columns]


def main():
    time1 = time.time()
    lst_min_max_year = [None, None, []]
    dfs_type1 = {}
    dfs_type2 = {}

    output_directory = 'processedData2'
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for fll_i, fll in enumerate(file_locations):
        print(f"fll_i {fll_i} | fll {fll}")
        lst_chunks_per_file = []

        for i, chunk in enumerate(pd.read_csv(fll, chunksize=10000, encoding='latin1')): #file_locations[3]
            
            # process "location column"
            if fll_i == 0:
                chunk.rename(columns={ "Country Name": "Location"}, inplace=True)

                value_column_name = ["Series Name Value" ]
                lst_min_max_year = min_max_years(chunk)
                chunk = process_chunk_by_year(chunk, int(lst_min_max_year[0]), int(lst_min_max_year[1]), lst_min_max_year[2], value_column_name[fll_i])
                #chunk.rename(columns={"Year": "Period"}, inplace=True)

            #process WHO files
            if fll_i == 1:

                if "First Tooltip" in chunk.columns and "Indicator" in chunk.columns:
                    new_column_name = chunk.iloc[0]["Indicator"]
                    chunk.rename(columns={"First Tooltip": new_column_name}, inplace=True)
                    chunk.drop(columns=['Indicator'], inplace=True)

                chunk.rename(columns={"Dim1": "Sexes"}, inplace=True)
                chunk.rename(columns={"Dim2": "Age Groups"}, inplace=True)

            #lst_chunks_per_file.append(chunk)
            base_filename = os.path.basename(fll)
            output_file_name = os.path.join(output_directory, f"{os.path.splitext(base_filename)[0]}_chunk_{i+1}.csv")
            while os.path.exists(output_file_name):
                output_file_name = os.path.join(output_directory, f"{os.path.splitext(base_filename)[0]}_chunk_{i+1}_new.csv")
        


            #output_file_name += fll[6:-4] + "_chunk" + str(i) + ".csv"
            chunk.to_csv(output_file_name, index=False)


if __name__ == "__main__":
    main()

