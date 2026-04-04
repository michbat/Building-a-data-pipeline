import sys
import pandas as pd



#
print(f"arguments: {sys.argv}")

#
month:int = int(sys.argv[1])
print(f"Month:{month}")

# 
df = pd.DataFrame({"day":[1,2], "num_passengers":[3,4]})
df['month'] = month
print(df.head())

# Parquet : format binaire des données, optimisé par rapport au csv

df.to_parquet(f"output_{month}.parquet")