import sys
import pandas as pd
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("demo").getOrCreate()

#####File Locations
fl_world_expenditure= "./data/WorldExpenditures.csv"
fl_world_development_indicator = "./data/world_development_indicators.csv"
fl_world_development_indicator_1 = "./data/world_development_indicators_1.csv"
fl_world_inflation = "./data/Global_Inflation.csv"

#####Column Definition
wExpendCols = ['Year', 'Country' ,'Sector', 'Expenditure(million USD)', 'GDP(%)']
wDevIndicatorCols = ["Country Name","Country Code","Series Name","Series Code" ,"1973 [YR1973]" ,"1974 [YR1974]","1975 [YR1975]","1976 [YR1975]","1977 [YR1975]","1978 [YR1975]","1979 [YR1975]","1980 [YR1975]","1981 [YR1975]","1982 [YR1975]","1983 [YR1975]","1984 [YR1975]","1985 [YR1985]","1986 [YR1986]","1987 [YR1987]","1988 [YR1988]","1989 [YR1989]","1990 [YR1990]","1991 [YR1991]","1992 [YR1992]","1993 [YR1993]","1994 [YR1994]","1995 [YR1995]","1996 [YR1996]","1997 [YR1997]","1998 [YR1998]","1999 [YR1999]","2000 [YR2000]","2001 [YR2001]","2002 [YR2002]","2003 [YR2003]","2004 [YR2004]","2005 [YR2005]","2006 [YR2006]","2007 [YR2007]","2008 [YR2008]","2009 [YR2009]","2010 [YR2010]","2011 [YR2011]","2012 [YR2012]","2013 [YR2013]","2014 [YR2014]","2015 [YR2015]","2016 [YR2016]","2017 [YR2017]","2018 [YR2018]","2019 [YR2019]","2020 [YR2020]","2021 [YR2021]","2022 [YR2022]"]
wGlobInflationCols = ["Country Code","IMF Country Code","Country","Indicator Type","Series Name","1970","1971","1972","1973" ,"1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","Note","","","",""]

#####Pandas dataframe
wExpenditure_panda_df = pd.read_csv(fl_world_expenditure, usecols= wExpendCols)
wDev_indicators_panda_df = pd.read_csv(fl_world_development_indicator, usecols= wDevIndicatorCols)
wDev_indicators_panda_df_1 = pd.read_csv(fl_world_development_indicator_1, usecols= wDevIndicatorCols)
wDev_indicators_panda_df.append(wDev_indicators_panda_df_1)

wInflation_panda_df = pd.read_csv(fl_world_inflation, usecols= wGlobInflationCols)

#####Pyspark dataframe
df_expenditure = spark.createDataFrame(wExpenditure_panda_df)
df_devIndicators = spark.createDataFrame(wDev_indicators_panda_df)
df_devIndicators = spark.createDataFrame(wInflation_panda_df)

#####Dataframe view
df_expenditure.show(1)
df_devIndicators.show(1)
df_devIndicators.show(1)





