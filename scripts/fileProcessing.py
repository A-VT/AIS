import sys
import pandas as pd
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("demo").getOrCreate()

#####File Locations
file_locations = [
    "./data/WorldExpenditures.csv", 
    "./data/world_development_indicators.csv",
    "./data/world_development_indicators_1.csv",
    "./data/Global_Inflation.csv",

    "./data/WHO_statistics/30-70cancerChdEtc.csv",
    "./data/WHO_statistics/adolescentBirthRate.csv",
    "./data/WHO_statistics/airPollutionDeathRate.csv",
    "./data/WHO_statistics/alcoholSubstanceAbuse.csv",
    "./data/WHO_statistics/atLeastBasicSanitizationServices.csv",
    "./data/WHO_statistics/basicDrinkingWaterServices.csv",
    "./data/WHO_statistics/basicHandWashing.csv",
    "./data/WHO_statistics/birthAttendedBySkilledPersonal.csv",
    "./data/WHO_statistics/cleanFuelAndTech.csv",
    "./data/WHO_statistics/crudeSuicideRates.csv",
    "./data/WHO_statistics/dataAvailibilityForUhc.csv",
    "./data/WHO_statistics/dentists.csv",
    "./data/WHO_statistics/eliminateViolenceAgainstWomen.csv",
    "./data/WHO_statistics/HALElifeExpectancyAtBirth.csv",
    "./data/WHO_statistics/HALeWHOregionLifeExpectancyAtBirth.csv",
    "./data/WHO_statistics/hepatitusBsurfaceAntigen.csv",
    "./data/WHO_statistics/incedenceOfMalaria.csv",
    "./data/WHO_statistics/incedenceOfTuberculosis.csv",
    "./data/WHO_statistics/infantMortalityRate.csv",
    "./data/WHO_statistics/interventionAgianstNTDs.csv",
    "./data/WHO_statistics/lifeExpectancyAtBirth.csv",
    "./data/WHO_statistics/maternalMortalityRatio.csv",
    "./data/WHO_statistics/medicalDoctors.csv",
    "./data/WHO_statistics/mortalityRatePoisoning.csv",
    "./data/WHO_statistics/mortalityRateUnsafeWash.csv",
    "./data/WHO_statistics/neonatalMortalityRate.csv",
    "./data/WHO_statistics/newHivInfections.csv",
    "./data/WHO_statistics/nursingAndMidwife.csv",
    "./data/WHO_statistics/ofHaleInLifeExpectancy.csv",
    "./data/WHO_statistics/pharmacists.csv",
    "./data/WHO_statistics/population10SDG3.8.2.csv",
    "./data/WHO_statistics/population25SDG3.8.2.csv",
    "./data/WHO_statistics/reproductiveAgeWomen.csv",
    "./data/WHO_statistics/roadTrafficDeaths.csv",
    "./data/WHO_statistics/safelySanitization.csv",
    "./data/WHO_statistics/tobaccoAge15.csv",
    "./data/WHO_statistics/uhcCoverage.csv",
    "./data/WHO_statistics/under5MortalityRate.csv",
    "./data/WHO_statistics/WHOregionLifeExpectancyAtBirth.csv"
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
    melted_chunk = pd.melt(chunk, id_vars=id_vars, value_vars=cols_to_melt, var_name="Year", value_name=newColName)
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
    lst_min_max_year = [None, None, []]
    for fll_i, fll in enumerate(file_locations):
        print(f"fll_i {fll_i} | fll {fll}")
        chunk_list = []
        toReplace = ["Country", "Country Name", "Country Name", "Country"]

        for i, chunk in enumerate(pd.read_csv(fll, chunksize=10000, encoding='latin1')): #file_locations[3]
            
            # process "location column"
            if fll_i <= 3:
                chunk.rename(columns={toReplace[fll_i]: "Location"}, inplace=True)

            #process year columns
            if fll_i == 0:
                chunk.rename(columns={"Year": "Period"}, inplace=True)
            if fll_i==1 or fll_i==2 or fll_i==3:
                value_column_name = ["Series Name Value", "Series Name Value", "Inflation Value" ]
                lst_min_max_year = min_max_years(chunk)
                chunk = process_chunk_by_year(chunk, int(lst_min_max_year[0]), int(lst_min_max_year[1]), lst_min_max_year[2], value_column_name[fll_i-1])
                chunk.rename(columns={"Year": "Period"}, inplace=True)

            #process WHO files
            if fll_i>3:
                if "First Tooltip" in chunk.columns and "Indicator" in chunk.columns:
                    print("First Tooltip in the columns")
                    print("Columns:", chunk.columns)
                    new_column_name = chunk.iloc[0]["Indicator"]
                    chunk.rename(columns={"First Tooltip": new_column_name}, inplace=True)
                    chunk.drop(columns=['Indicator'], inplace=True)
                    
                if "Dim1" in chunk.columns:
                    chunk.rename(columns={"Dim1": "Dimension"}, inplace=True)


            
            print(f"chunk {i}")
            print(chunk.head())
            chunk_list.append(chunk)

 #       df_long = pd.concat(chunk_list)
    #print(df_long.head())






if __name__ == "__main__":
    main()

#####Pyspark dataframe
#df_expenditure = spark.createDataFrame(wExpenditure_panda_df)
#df_devIndicators = spark.createDataFrame(wDev_indicators_panda_df)
#df_inflation = spark.createDataFrame(wInflation_panda_df)

#####Dataframe view
#df_expenditure.show(1)
#df_devIndicators.show(1)
#df_inflation.show(1)
