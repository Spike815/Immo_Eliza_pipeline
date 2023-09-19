import pandas as pd

data=pd.read_csv("working/utls/Belgium_Postalcode.csv")
sing = data.to_csv()
print(type(sing))
