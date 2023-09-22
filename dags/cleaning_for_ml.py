import pandas as pd
from machine_learning_model import build_ml

#define the function to remove outliners
def remove_outliners(df,columns,n_std):
    for col in columns:
        mean = df[col].mean()
        sd = df[col].std()
        df = df[(df[col] <= mean+(n_std*sd))]
        df = df[(df[col] >= mean-(n_std*sd))]

    return df
#function to convert the postalcode to two digits
def convert(n):
    return int(n/100)

#main cleaning function

def cleaning_for_ml(df):
    #drop Nan rows
    df["kitchen"]=df["kitchen"].fillna("Not equipped ")

    #remove castles
    castle = df[df["subtype of property"]=="CASTLE"].index
    df.drop(castle,inplace=True)

    #get two digits
    df["digit"]=df["postalCode"].agg(convert)

    df.drop(columns=["fully equipped kitchen"],inplace=True)

    #replace True with 1, and False with 0, turn them into numerical data
    # correct Liege
    df.replace([True,False,"Liège"],[1,0,"Liege"],inplace=True)

    df = remove_outliners(df, ["number of bedrooms","livable area","living area",
                           "total property area","garden area","total land area","terrace area","price"],3)
    #change the building-state description
    df["state of the building"].replace(['good', 'as_new', 'to_be_done_up', 
                                        'to_renovate', 'to_restore',
                                        'just_renovated', '0'],
                                        ["GOOD","NEW","TO RENOVATE","TO RENOVATE","TO REBUILD"
                                            ,"JUST RENOVATED",0],
                                        inplace=True)
    df = df.drop(df[df["state of the building"]==0].index)

    return df


df = pd.read_csv("/home/bo/study/becode/Immoweb_application/cleaned_data.csv")
df=cleaning_for_ml(df)
build_ml(df)