import pandas as pd

df1 = pd.read_csv("data/world_development_indicators.csv", encoding='latin1')
df2 = pd.read_csv("data/world_development_indicators_1.csv", encoding='latin1')
combined_df = pd.concat([df1, df2], ignore_index=True)
combined_df.to_csv('./data/combined_world_development.csv', index=False)