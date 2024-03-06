import sys
import pandas as pd
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("demo").getOrCreate()

#####File Locations
file_locations = ["./data/WorldExpenditures.csv", "./data/world_development_indicators.csv" , 
                  "./data/world_development_indicators_1.csv" , "./data/Global_Inflation.csv",
                  "./data/WHO statistics/30-70cancerChdEtc.csv", "./data/WHO statistics/adolescentBirthRate.csv",
                  "./data/WHO statistics/airPollutionDeathRate.csv", "./data/WHO statistics/alcoholSubstanceAbuse.csv",
                  "./data/WHO statistics/atLeastBasicSanitizationServices.csv",
                  "./data/WHO statistics/basicDrinkingWaterServices.csv", "./data/WHO statistics/basicHandWashing.csv",
                  "./data/WHO statistics/birthAttendedBySkilledPersonal.csv",
                  "./data/WHO statistics/cleanFuelAndTech.csv", "data/WHO statistics/crudeSuicideRates.csv", 
                  "./data/WHO statistics/dataAvailibilityForUhc.csv", "data/WHO statistics/dentists.csv",
                  "./data/WHO statistics/eliminateViolenceAgainstWomen.csv",
                  "./data/WHO statistics/HALElifeExpectancyAtBirth.csv",
                  "./data/WHO statistics/HALeWHOregionLifeExpectancyAtBirth.csv",
                  "./data/WHO statistics/hepatitusBsurfaceAntigen.csv", "./data/WHO statistics/incedenceOfMalaria.csv",
                  "./data/WHO statistics/incedenceOfTuberculosis.csv","./data/WHO statistics/infantMortalityRate.csv",
                  "./data/WHO statistics/interventionAgianstNTDs.csv","./data/WHO statistics/lifeExpectancyAtBirth.csv",
                  "./data/WHO statistics/maternalMortalityRatio.csv", "./data/WHO statistics/medicalDoctors.csv",
                  "./data/WHO statistics/mortalityRatePoisoning.csv",
                  "./data/WHO statistics/mortalityRateUnsafeWash.csv",
                  "./data/WHO statistics/neonatalMortalityRate.csv", "./data/WHO statistics/newHivInfections.csv",
                  "./data/WHO statistics/nursingAndMidwife.csv", "./data/WHO statistics/ofHaleInLifeExpectancy.csv",
                  "./data/WHO statistics/pharmacists.csv", "./data/WHO statistics/population10SDG3.8.2.csv",
                  "./data/WHO statistics/population25SDG3.8.2.csv", "./data/WHO statistics/reproductiveAgeWomen.csv",
                  "./data/WHO statistics/roadTrafficDeaths.csv", "./data/WHO statistics/safelySanitization.csv",
                  "./data/WHO statistics/tobaccoAge15.csv", "./data/WHO statistics/uhcCoverage.csv",
                  "./data/WHO statistics/under5MortalityRate.csv",
                  "./data/WHO statistics/WHOregionLifeExpectancyAtBirth.csv"]

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


def process_chunk_by_year(chunk, first_year, last_year, cols):
    years_columns = [str(year) for year in range(first_year, last_year + 1)]
    cols_to_melt = [col for col in years_columns if col in chunk.columns]
    if not cols_to_melt:
        #print("No year columns found in the chunk.")
        return None
    
    id_vars = [col for col in chunk.columns if col not in cols_to_melt]
    melted_chunk = pd.melt(chunk, id_vars=id_vars, value_vars=cols_to_melt, var_name="Year", value_name="Value")
    print(melted_chunk.columns)
    return melted_chunk


def min_max_years(chunk):
    years_columns, other_columns = [], []
    for col in chunk.columns:
        if any(char.isdigit() for char in col) and 'Unnamed' not in col:
            clean_col = col.split(' ')[0]
            years_columns.append(clean_col)
        else:
            other_columns.append(col)

    renamed_chunk = chunk.rename(columns={col: col.split(' ')[0] for col in years_columns})

    min_year_col = min(years_columns) if years_columns else None
    max_year_col = max(years_columns) if years_columns else None

    return [min_year_col, max_year_col, other_columns]


def main():
    lst_min_max_year = [None, None, []]
    for fll_i, fll in enumerate(file_locations):

        chunk_list = []
        default_location_column_name, colName = "CountryName" , ""

        for i, chunk in enumerate(pd.read_csv(fll, chunksize=10000, encoding='latin1')): #file_locations[3]

            #process year columns
            if fll_i==1 or fll_i==2 or fll_i==3:
                lst_min_max_year = min_max_years(chunk)
                #print(f"{fll} {lst_min_max_year}")
                chunk_processed = process_chunk_by_year(chunk, int(lst_min_max_year[0]), int(lst_min_max_year[1]), lst_min_max_year[2])
                chunk_list.append(chunk_processed)

    #    df_long = pd.concat(chunk_list)
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
