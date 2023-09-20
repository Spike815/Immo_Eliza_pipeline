import pandas as pd

list = ["2","hello","test"]
x = pd.DataFrame({"list":list})
print(x)
list1 = x["list"].tolist()
print(list1)