from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder,MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib
from xgboost import XGBRegressor
import tempfile
from upload_to_s3 import s3_obj

def build_ml(df):
    columns_to_drop = ["id","locality","street","price","type of sale"
                   ,"Density","Area(km2)",
                   "Population","price per sqr","open fire","postalCode","Gemeente",
                   "livable area","latitude","longitude","subtype of property"]
    #take price as the target
    y = df.loc[:,"price"].to_numpy().reshape(-1,1)
    #rename the columns
    X=df.drop(columns=columns_to_drop,axis=1)
    column_list = []
    for column in X.columns:
        column = column.replace(" ","_")
        column_list.append(column)
    X.columns = column_list
    X = X.to_numpy()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size= 0.2,random_state=21)

    #Using Columntransformer for pipeline OHE
    trans_1 = ColumnTransformer([('ohe_trans',
                                  OneHotEncoder(sparse_output=False, handle_unknown='ignore'), 
                                  [0,12,13,14,15])], 
                                  remainder='passthrough')
    
    #using columntransformer for scaling
    trans_2 = ColumnTransformer([('scale', MinMaxScaler(),slice(0,len(X)))], 
                                remainder='passthrough')
    
    # build up xgboost model
    model= XGBRegressor()
    pipe = Pipeline([('trans_1',trans_1),
                    ("trans_2",trans_2),
                    ('trans_3',model)])

    pipe.fit(X_train, y_train)
    with tempfile.TemporaryFile() as fp:
        joblib.dump(pipe,fp)
        fp.seek(0)
        s3 = s3_obj()
        key="ml_model/XGB.joblib"
        s3.put_object(Body=fp.read(),Bucket = 'immostudy-temp',Key=key)
    
build_ml()