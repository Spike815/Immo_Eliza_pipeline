import pandas as pd
from pathlib import Path
import numpy as np

def cleaning_data(data):
    #initial cleaning
    data.drop_duplicates("id")
    data = data[data['price']>0]
    data["postalCode"]=data["postalCode"].astype(str)

    #import the exel file of postalcode and clean the data
    path = Path.cwd() / "dags/utls/Belgium_Postalcode.csv"
    post_code = pd.read_csv(path)

    #get population info
    population_info={"Province":["Antwerpen","Oost-Vlaanderen","Vlaams-Brabant","Limburg","Hainaut","West-Vlaanderen","Liège","Luxembourg","Namur","Brabant wallon","Brussel"],
                 "Population":[1857986,1515064,1146175,874048,1196796,1344241,1106992,284638,494325,403599,1208542],
                 "Area(km2)":[2876,3007,2118,2427,3197,3813,3857,4459,3675,1097,162.4],
                 "Density":[647,504,542,361,375,353,288,63,135,368,7442]}
    population_df=pd.DataFrame(population_info)

    #merge postalcode data to original dataframe
    data = data.merge(post_code,how="left",on="postalCode")
    data =data.merge(population_df,on="Province",how="left")
    data = data.drop(columns=["province"])

    #fill the nan with false in the following columns
    for column in ["furnished","open fire","terrace","garden","swimming pool"]:
        data.loc[:,column]=data[column].fillna(False)

    # fill the livable area with nan for the correlation calculation purpose
    data["livable area"] = data["livable area"].replace(0, np.nan)

    #fill the nan with o in the following columns:
    for column in ["terrace area","garden area","total land area","number of facades"]:
        data.loc[:,column]=data[column].fillna(0)
    data = data.dropna(subset=["Province",'total property area'])
    data["garden"]=data["garden"].astype(bool)

    #clean up kitchen discription
    kitchen_dict = {"fully equipped kitchen":["usa_hyper_equipped ","hyper_equipped","usa_installed","semi_equipped","usa_semi_equipped","not_installed","usa_uninstalled",0],
                    "kitchen":["Hyper equipped","Hyper equipped","Equipped","Semi equipped","Semi equipped","Not equipped","Not equipped","Not equipped"]}
    kitchen_df=pd.DataFrame(kitchen_dict)
    data = data.merge(kitchen_df,how="left",on="fully equipped kitchen")
    
    #filter out the duplicates in the list
    condition = data[["latitude","longitude","street","price","total property area"]].duplicated(keep=False)

    #pass the returned boolean to data, get the index of the duplicates
    data = data.drop(data[condition].index)
    data = data[data["total property area"]>0]
    data["price per sqr"]=data["price"]/data["total property area"]

    print(data.head(5))

