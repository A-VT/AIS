import sys
import pandas as pd
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("demo").getOrCreate()

#####File Locations
file_locations = ["./data/WorldExpenditures.csv", "./data/world_development_indicators.csv" , 
                  "./data/world_development_indicators_1.csv" , "./data/Global_Inflation.csv",
                  "./data/WHO statistics/30-70cancerChdEtc.csv", "./data/WHO statistics/adolescentBirthRate.csv",
                  "./data/WHO statistics/airPollutionDeathRate.py", "./data/WHO statistics/alcoholSubstanceAbuse.py",
                  "./data/WHO statistics/atLeastBasicSanitizationServices.py",
                  "./data/WHO statistics/basicDrinkingWaterServices.py", "./data/WHO statistics/basicHandWashing.csv",
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


def process_chunk_by_year(chunk, first_year, last_year):
    years_columns = [str(year) for year in range(first_year, last_year)]
    chunk_long = pd.melt(chunk, id_vars=["Country Code","IMF Country Code","Country","Indicator Type","Series Name", "Note"], 
                         value_vars=years_columns, var_name="Year", value_name="Value")
    return chunk_long


def min_max_years(chunk):
    years_columns = [col for col in chunk.columns if col.isdigit()]
    return [min(years_columns), max(years_columns)]


def main():
    lst_min_max_year = []

    for fll_i, fll in enumerate(file_locations):
        chunk_list = []
        for i, chunk in enumerate(pd.read_csv(fll_i, chunksize=10000, encoding='latin1')): #file_locations[3]
            if i==0:
                lst_min_max_year = min_max_years(chunk)
            chunk_processed = process_chunk_by_year(chunk, int(lst_min_max_year[0]), int(lst_min_max_year[1]))
            chunk_list.append(chunk_processed)
        df_long = pd.concat(chunk_list)



    print(df_long.head())






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
